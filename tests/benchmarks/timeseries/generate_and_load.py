#!/usr/bin/env python3
"""Generate the deterministic Kubernetes time-series corpus and load it into
ClickHouse via the Prometheus remote-write protocol.

Frames are generated on the fly (nothing is materialized on disk) and POSTed
in order to the `prometheus_api_v1` handler; see README.md for the required
server configuration. Generation runs in parallel worker processes; POSTs are
serial, and the reported load time counts POST wall-clock only.

Dependencies: protobuf, python-snappy, requests.

Example:

    ./generate_and_load.py --scale 1 --url http://localhost:8123/prometheus/api/v1/write
"""

import argparse
import sys
import time

import prom_io
import stream_gen

# 2024-01-01 00:00:00 UTC; 30 days of 15 s steps => 172800 steps,
# window end 1706659185. The queries in queries/ hardcode evaluation
# times inside this window, so keep --start/--step/--days at defaults
# unless you regenerate the queries too.
DEFAULT_START = 1704067200
DEFAULT_STEP = 15
DEFAULT_DAYS = 30.0


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--url",
        default="http://localhost:8123/prometheus/api/v1/write",
        help="remote-write endpoint (default: %(default)s)",
    )
    ap.add_argument(
        "--scale",
        type=int,
        default=1,
        help="cardinality multiplier; scale=1 is 6112 series / ~1.06B samples "
        "over 30 days (default: %(default)s)",
    )
    ap.add_argument(
        "--days",
        type=float,
        default=DEFAULT_DAYS,
        help="window length in days (default: %(default)s; queries assume 30)",
    )
    ap.add_argument(
        "--step",
        type=int,
        default=DEFAULT_STEP,
        help="scrape interval in seconds (default: %(default)s)",
    )
    ap.add_argument(
        "--start",
        type=int,
        default=DEFAULT_START,
        help="window start, unix seconds (default: %(default)s = 2024-01-01 UTC)",
    )
    ap.add_argument(
        "--frame-steps",
        type=int,
        default=30,
        help="steps per remote-write frame (default: %(default)s)",
    )
    ap.add_argument("--seed", type=int, default=1729)
    ap.add_argument(
        "--workers",
        type=int,
        default=None,
        help="parallel frame-generation workers (default: half cpu count)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="print corpus stats and exit without generating or loading",
    )
    args = ap.parse_args()

    knobs = dict(
        seed=args.seed,
        scale=args.scale,
        start=args.start,
        step=args.step,
        days=args.days,
        num_steps=0,
        frame_steps=args.frame_steps,
    )
    model, specs, meta = stream_gen.build_corpus(knobs)
    print(
        f"corpus: series={meta['series_count']:,} samples={meta['total_samples']:,} "
        f"frames={meta['num_frames']} window=[{meta['start_unix']}, {meta['end_unix']}] "
        f"step={meta['step_seconds']}s scale={args.scale} seed={args.seed}",
        file=sys.stderr,
        flush=True,
    )
    if args.dry_run:
        return

    if args.workers is None:
        import os

        n = os.cpu_count() or 1
        workers = max(1, (n + 1) // 2)
    else:
        workers = max(1, args.workers)
    print(f"gen_workers={workers}", file=sys.stderr, flush=True)

    expected_frames = meta["num_frames"]
    post_time = 0.0
    num_frames = 0
    t_wall0 = time.perf_counter()
    for body in stream_gen.iter_frames_parallel(
        knobs,
        meta["start_unix"],
        meta["step_seconds"],
        meta["num_steps"],
        meta["frame_steps"],
        workers=workers,
        model=model,
        specs=specs,
    ):
        t0 = time.perf_counter()
        resp = prom_io.post_remote_write(args.url, body)
        prom_io.check_remote_write_response(resp)
        post_time += time.perf_counter() - t0
        num_frames += 1
        if num_frames % 100 == 0 or num_frames == expected_frames:
            wall = time.perf_counter() - t_wall0
            print(
                f"  progress: frame {num_frames}/{expected_frames} "
                f"({100.0 * num_frames / expected_frames:.1f}%) "
                f"post_s={post_time:.1f} wall_s={wall:.1f}",
                file=sys.stderr,
                flush=True,
            )

    total_samples = meta["total_samples"]
    sps = total_samples / post_time if post_time > 0 else float("inf")
    print(
        f"done: {total_samples:,} samples in {num_frames} frames; "
        f"post_time={post_time:.1f}s ({sps:,.0f} samples/sec)",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    main()
