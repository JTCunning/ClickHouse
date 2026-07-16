"""Streaming Kubernetes time-series corpus.

Builds topology + sorted series descriptors once (~6k series in RAM at
scale=1), then emits remote-write frames for arbitrary time windows without
materializing full sample histories. Used by generate_and_load.py
(stream-during-load).
"""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import prom_io

# ---------------------------------------------------------------------------
# Fixed vocabulary / job labels (match the mixin selectors).
# ---------------------------------------------------------------------------
CLUSTER = "bench"
JOB_CADVISOR = "cadvisor"
JOB_KSM = "kube-state-metrics"
JOB_NODE = "node-exporter"

NONIDLE_MODES = ["user", "system", "iowait", "irq", "softirq", "steal", "nice"]
POD_PHASES = ["Pending", "Running", "Succeeded", "Failed", "Unknown"]

GIB = 1024 ** 3
MIB = 1024 ** 2


def stable_hash(*parts):
    """Deterministic non-negative int from the given parts (seed-mixed)."""
    h = hashlib.sha256("\x1f".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big")


def _hex(*parts, n=5):
    return f"{stable_hash(*parts):x}"[:n].rjust(n, "0")


class ClusterModel:
    def __init__(self, args):
        self.seed = args.seed
        self.cluster = CLUSTER
        self.cpus_per_node = args.cpus_per_node

        pods_per_ns = args.pods_per_ns * max(1, args.scale)
        deployments_per_ns = args.workloads_per_ns or max(1, pods_per_ns // 5)

        self.nodes = [f"node-{n}" for n in range(args.nodes)]
        self.namespaces = []
        self.pods = []
        self.replicasets = []

        gidx = 0
        for m in range(args.namespaces):
            ns = f"ns-{m}"
            self.namespaces.append(ns)
            deployments = []
            for d in range(deployments_per_ns):
                deploy = f"{ns}-app-{d}"
                rs = f"{deploy}-{_hex(self.seed, ns, 'rs', d)}"
                deployments.append((deploy, rs))
                self.replicasets.append(
                    {"namespace": ns, "replicaset": rs, "deployment": deploy}
                )
            for p in range(pods_per_ns):
                deploy, rs = deployments[p % len(deployments)]
                pod = f"{rs}-{_hex(self.seed, ns, 'pod', p)}"
                node = self.nodes[gidx % len(self.nodes)]
                gidx += 1
                phase = (
                    "Pending"
                    if (stable_hash(self.seed, pod, "phase") % 20 == 0)
                    else "Running"
                )
                containers = []
                for c in range(args.containers_per_pod):
                    containers.append(
                        {
                            "container": f"container-{c}",
                            "image": f"registry.k8s.io/app-{c}:v1.0",
                        }
                    )
                self.pods.append(
                    {
                        "namespace": ns,
                        "pod": pod,
                        "node": node,
                        "deployment": deploy,
                        "replicaset": rs,
                        "phase": phase,
                        "containers": containers,
                    }
                )

    def container_cpu_rate(self, ns, pod, container):
        return (stable_hash(self.seed, ns, pod, container, "cpu_rate") % 50 + 1) / 100.0

    def container_mem_working_set(self, ns, pod, container, i, step):
        # Constant per-series gauge (no sine). Sine gauges caused ClickHouse
        # PromQL aggregations to diverge from Prometheus on fingerprint compare;
        # counters already match bit-for-bit with linear slopes.
        _ = (i, step)
        return float(
            (stable_hash(self.seed, ns, pod, container, "ws_base") % 449 + 64) * MIB
        )

    def container_cpu_request(self, ns, pod, container):
        return (stable_hash(self.seed, ns, pod, container, "cpu_req") % 5 + 1) / 10.0

    def container_mem_request(self, ns, pod, container):
        return (stable_hash(self.seed, ns, pod, container, "mem_req") % 8 + 1) * 64 * MIB

    def node_nonidle_slopes(self, node, cpu):
        slopes = {}
        for mode in NONIDLE_MODES:
            slopes[mode] = (stable_hash(self.seed, node, cpu, mode) % 5 + 1) / 100.0
        return slopes

    def node_mem_total(self, node):
        return 32 * GIB

    def node_mem_available(self, node, i, step):
        _ = (i, step)
        total = self.node_mem_total(node)
        frac = 0.3 + (stable_hash(self.seed, node, "mem_avail") % 40) / 100.0
        return float(total * frac)


def _value_at(model, kind, params, i, step):
    """O(1) sample value for series descriptor (kind, params) at step index i."""
    if kind == "const":
        return float(params["v"])
    if kind == "linear":
        return float(params["slope"]) * (i * step)
    if kind == "ws":
        return model.container_mem_working_set(
            params["ns"], params["pod"], params["container"], i, step
        )
    if kind == "ws_frac":
        ws = model.container_mem_working_set(
            params["ns"], params["pod"], params["container"], i, step
        )
        return float(params["frac"]) * ws
    if kind == "mem_avail":
        return model.node_mem_available(params["node"], i, step)
    if kind == "mem_avail_sum":
        total = 0.0
        for node in model.nodes:
            total += model.node_mem_available(node, i, step)
        return total
    raise ValueError(f"unknown series kind {kind!r}")


def iter_series_specs(model, step):
    """Yield (labels, kind, params) for every series (unsorted)."""
    cl = {"cluster": model.cluster}

    # --- cAdvisor -----------------------------------------------------------
    for pod in model.pods:
        ns, pname = pod["namespace"], pod["pod"]
        node = pod["node"]
        cadvisor_instance = f"{node}:10250"
        for ctr in pod["containers"]:
            cname, image = ctr["container"], ctr["image"]
            base = dict(
                cl,
                namespace=ns,
                pod=pname,
                container=cname,
                image=image,
                instance=cadvisor_instance,
                job=JOB_CADVISOR,
            )
            r = model.container_cpu_rate(ns, pname, cname)
            yield (
                dict(base, __name__="container_cpu_usage_seconds_total"),
                "linear",
                {"slope": r},
            )
            ws_p = {"ns": ns, "pod": pname, "container": cname}
            yield (dict(base, __name__="container_memory_working_set_bytes"), "ws", ws_p)
            yield (
                dict(base, __name__="container_memory_rss"),
                "ws_frac",
                {**ws_p, "frac": 0.75},
            )
            yield (
                dict(base, __name__="container_memory_cache"),
                "ws_frac",
                {**ws_p, "frac": 0.15},
            )
            yield (dict(base, __name__="container_memory_swap"), "const", {"v": 0.0})

    # --- kube-state-metrics -------------------------------------------------
    for pod in model.pods:
        ns, pname, node = pod["namespace"], pod["pod"], pod["node"]
        ksm = {"job": JOB_KSM}
        yield (
            dict(
                cl,
                **ksm,
                __name__="kube_pod_info",
                namespace=ns,
                pod=pname,
                node=node,
                created_by_kind="ReplicaSet",
                created_by_name=pod["replicaset"],
                host_network="false",
            ),
            "const",
            {"v": 1.0},
        )
        for phase in POD_PHASES:
            yield (
                dict(
                    cl,
                    **ksm,
                    __name__="kube_pod_status_phase",
                    namespace=ns,
                    pod=pname,
                    phase=phase,
                ),
                "const",
                {"v": 1.0 if phase == pod["phase"] else 0.0},
            )
        yield (
            dict(
                cl,
                **ksm,
                __name__="kube_pod_owner",
                namespace=ns,
                pod=pname,
                owner_kind="ReplicaSet",
                owner_name=pod["replicaset"],
                owner_is_controller="true",
            ),
            "const",
            {"v": 1.0},
        )
        for ctr in pod["containers"]:
            cname = ctr["container"]
            cpu_req = model.container_cpu_request(ns, pname, cname)
            mem_req = model.container_mem_request(ns, pname, cname)
            rl = dict(cl, **ksm, namespace=ns, pod=pname, container=cname, node=node)
            yield (
                dict(rl, __name__="kube_pod_container_resource_requests", resource="cpu", unit="core"),
                "const",
                {"v": cpu_req},
            )
            yield (
                dict(rl, __name__="kube_pod_container_resource_limits", resource="cpu", unit="core"),
                "const",
                {"v": cpu_req * 2},
            )
            yield (
                dict(
                    rl,
                    __name__="kube_pod_container_resource_requests",
                    resource="memory",
                    unit="byte",
                ),
                "const",
                {"v": mem_req},
            )
            yield (
                dict(
                    rl,
                    __name__="kube_pod_container_resource_limits",
                    resource="memory",
                    unit="byte",
                ),
                "const",
                {"v": mem_req * 2},
            )

    for rs in model.replicasets:
        yield (
            dict(
                cl,
                job=JOB_KSM,
                __name__="kube_replicaset_owner",
                namespace=rs["namespace"],
                replicaset=rs["replicaset"],
                owner_kind="Deployment",
                owner_name=rs["deployment"],
                owner_is_controller="true",
            ),
            "const",
            {"v": 1.0},
        )

    for node in model.nodes:
        yield (
            dict(
                cl,
                job=JOB_KSM,
                __name__="kube_node_status_allocatable",
                node=node,
                resource="cpu",
                unit="core",
            ),
            "const",
            {"v": float(model.cpus_per_node)},
        )
        yield (
            dict(
                cl,
                job=JOB_KSM,
                __name__="kube_node_status_allocatable",
                node=node,
                resource="memory",
                unit="byte",
            ),
            "const",
            {"v": model.node_mem_total(node) * 0.95},
        )
        yield (
            dict(
                cl,
                job=JOB_KSM,
                __name__="kube_node_status_capacity",
                node=node,
                resource="cpu",
                unit="core",
            ),
            "const",
            {"v": float(model.cpus_per_node)},
        )
        yield (
            dict(
                cl,
                job=JOB_KSM,
                __name__="kube_node_status_capacity",
                node=node,
                resource="memory",
                unit="byte",
            ),
            "const",
            {"v": float(model.node_mem_total(node))},
        )

    # --- node-exporter ------------------------------------------------------
    for node in model.nodes:
        ne_instance = f"{node}:9100"
        for cpu in range(model.cpus_per_node):
            slopes = model.node_nonidle_slopes(node, cpu)
            idle = 1.0 - sum(slopes.values())
            for mode, slope in list(slopes.items()) + [("idle", idle)]:
                yield (
                    dict(
                        cl,
                        __name__="node_cpu_seconds_total",
                        instance=ne_instance,
                        job=JOB_NODE,
                        cpu=str(cpu),
                        mode=mode,
                    ),
                    "linear",
                    {"slope": slope},
                )
        yield (
            dict(cl, __name__="node_memory_MemTotal_bytes", instance=ne_instance, job=JOB_NODE),
            "const",
            {"v": model.node_mem_total(node)},
        )
        yield (
            dict(
                cl,
                __name__="node_memory_MemAvailable_bytes",
                instance=ne_instance,
                job=JOB_NODE,
            ),
            "mem_avail",
            {"node": node},
        )
        load = 0.5 + (stable_hash(model.seed, node, "load") % 200) / 100.0
        yield (
            dict(cl, __name__="node_load1", instance=ne_instance, job=JOB_NODE),
            "const",
            {"v": load},
        )

    # --- Pre-recorded mixin series ------------------------------------------
    for pod in model.pods:
        ns, pname, node = pod["namespace"], pod["pod"], pod["node"]
        for ctr in pod["containers"]:
            cname = ctr["container"]
            r = model.container_cpu_rate(ns, pname, cname)
            yield (
                dict(
                    cl,
                    __name__="node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m",
                    namespace=ns,
                    pod=pname,
                    container=cname,
                    node=node,
                ),
                "const",
                {"v": r},
            )

    total_cpus = len(model.nodes) * model.cpus_per_node
    total_nonidle = 0.0
    for node in model.nodes:
        for cpu in range(model.cpus_per_node):
            total_nonidle += sum(model.node_nonidle_slopes(node, cpu).values())
    ratio = (total_nonidle / total_cpus) if total_cpus else 0.0
    yield (dict(cl, __name__="cluster:node_cpu:ratio_rate5m"), "const", {"v": ratio})

    cpu_req = {ns: 0.0 for ns in model.namespaces}
    cpu_lim = {ns: 0.0 for ns in model.namespaces}
    mem_req = {ns: 0.0 for ns in model.namespaces}
    mem_lim = {ns: 0.0 for ns in model.namespaces}
    for pod in model.pods:
        if pod["phase"] not in ("Pending", "Running"):
            continue
        ns, pname = pod["namespace"], pod["pod"]
        for ctr in pod["containers"]:
            cname = ctr["container"]
            cr = model.container_cpu_request(ns, pname, cname)
            mr = model.container_mem_request(ns, pname, cname)
            cpu_req[ns] += cr
            cpu_lim[ns] += cr * 2
            mem_req[ns] += mr
            mem_lim[ns] += mr * 2
    for ns in model.namespaces:
        yield (
            dict(cl, __name__="namespace_cpu:kube_pod_container_resource_requests:sum", namespace=ns),
            "const",
            {"v": cpu_req[ns]},
        )
        yield (
            dict(cl, __name__="namespace_cpu:kube_pod_container_resource_limits:sum", namespace=ns),
            "const",
            {"v": cpu_lim[ns]},
        )
        yield (
            dict(
                cl,
                __name__="namespace_memory:kube_pod_container_resource_requests:sum",
                namespace=ns,
            ),
            "const",
            {"v": mem_req[ns]},
        )
        yield (
            dict(
                cl,
                __name__="namespace_memory:kube_pod_container_resource_limits:sum",
                namespace=ns,
            ),
            "const",
            {"v": mem_lim[ns]},
        )

    yield (dict(cl, __name__=":node_memory_MemAvailable_bytes:sum"), "mem_avail_sum", {})

    for pod in model.pods:
        yield (
            dict(
                cl,
                __name__="namespace_workload_pod:kube_pod_owner:relabel",
                namespace=pod["namespace"],
                workload=pod["deployment"],
                workload_type="deployment",
                pod=pod["pod"],
            ),
            "const",
            {"v": 1.0},
        )


def build_sorted_specs(model, step):
    """Materialize sorted (labels, kind, params) list — O(series), not O(samples)."""
    specs = list(iter_series_specs(model, step))
    specs.sort(key=lambda t: tuple(sorted(t[0].items())))
    return specs


def resolve_num_steps(days, step, num_steps):
    if days and days > 0:
        return max(2, round(days * 86400 / step))
    return num_steps


def args_from_mapping(m):
    """Build a SimpleNamespace for ClusterModel from a dict / argparse.Namespace."""
    if isinstance(m, SimpleNamespace):
        return m
    if hasattr(m, "__dict__") and not isinstance(m, dict):
        return m
    defaults = dict(
        seed=1729,
        scale=1,
        nodes=10,
        namespaces=20,
        pods_per_ns=10,
        containers_per_pod=2,
        cpus_per_node=4,
        workloads_per_ns=0,
        start=1704067200,
        step=15,
        num_steps=240,
        days=0.0,
        frame_steps=30,
    )
    defaults.update(m)
    return SimpleNamespace(**defaults)


def build_corpus(args):
    """Return (model, specs, meta) for the given knobs.

    meta includes window / count fields used by the manifest and loader.
    """
    args = args_from_mapping(args)
    num_steps = resolve_num_steps(args.days, args.step, args.num_steps)
    model = ClusterModel(args)
    specs = build_sorted_specs(model, args.step)
    end_unix = args.start + (num_steps - 1) * args.step
    window_seconds = (num_steps - 1) * args.step
    num_frames = (num_steps + args.frame_steps - 1) // args.frame_steps
    series_count = len(specs)
    meta = {
        "seed": args.seed,
        "scale": args.scale,
        "cluster": model.cluster,
        "knobs": {
            "nodes": args.nodes,
            "namespaces": args.namespaces,
            "pods_per_ns": args.pods_per_ns,
            "containers_per_pod": args.containers_per_pod,
            "cpus_per_node": args.cpus_per_node,
            "workloads_per_ns": args.workloads_per_ns,
            "scale": args.scale,
            "seed": args.seed,
            "start": args.start,
            "step": args.step,
            "num_steps": num_steps,
            "days": args.days if args.days else 0.0,
            "frame_steps": args.frame_steps,
        },
        "topology": {
            "nodes": len(model.nodes),
            "namespaces": len(model.namespaces),
            "pods": len(model.pods),
            "replicasets": len(model.replicasets),
            "containers_per_pod": args.containers_per_pod,
            "cpus_per_node": args.cpus_per_node,
        },
        "start_unix": args.start,
        "step_seconds": args.step,
        "num_steps": num_steps,
        "end_unix": end_unix,
        "window_seconds": window_seconds,
        "days": round(window_seconds / 86400.0, 4),
        "frame_steps": args.frame_steps,
        "num_frames": num_frames,
        "series_count": series_count,
        "total_samples": series_count * num_steps,
        "metrics": sorted({labels["__name__"] for labels, _, _ in specs}),
        "mode": "stream",
    }
    return model, specs, meta


def build_one_frame(model, specs, start, step, a, b, time_shift_ms=0):
    """Build one snappy WriteRequest body for step indices [a, b)."""
    frame_series = []
    for labels, kind, params in specs:
        samples = []
        for i in range(a, b):
            ts_ms = int((start + i * step) * 1000) + time_shift_ms
            samples.append((ts_ms, _value_at(model, kind, params, i, step)))
        frame_series.append((labels, samples))
    wr = prom_io.build_write_request(frame_series)
    return prom_io.serialize_remote_write(wr)


def iter_frames(model, specs, start, step, num_steps, frame_steps, time_shift_ms=0):
    """Yield snappy-compressed WriteRequest bodies for successive time frames.

    Timestamps are absolute ms: (start + i*step)*1000 + time_shift_ms.
    Generation is O(series * frame_steps) RAM per frame — never full history.
    """
    for a in range(0, num_steps, frame_steps):
        b = min(a + frame_steps, num_steps)
        yield build_one_frame(model, specs, start, step, a, b, time_shift_ms)


# ---------------------------------------------------------------------------
# Parallel frame generation (ProcessPool). Workers rebuild corpus from knobs.
# ---------------------------------------------------------------------------
_POOL_STATE = {}


def _pool_init(knobs, start, step, time_shift_ms):
    model, specs, _meta = build_corpus(knobs)
    _POOL_STATE["model"] = model
    _POOL_STATE["specs"] = specs
    _POOL_STATE["start"] = start
    _POOL_STATE["step"] = step
    _POOL_STATE["time_shift_ms"] = time_shift_ms


def _pool_build_frame(ab):
    a, b = ab
    return build_one_frame(
        _POOL_STATE["model"],
        _POOL_STATE["specs"],
        _POOL_STATE["start"],
        _POOL_STATE["step"],
        a,
        b,
        _POOL_STATE["time_shift_ms"],
    )


def iter_frames_parallel(
    knobs,
    start,
    step,
    num_steps,
    frame_steps,
    time_shift_ms=0,
    workers=None,
    model=None,
    specs=None,
):
    """Yield frames in order, generating them across ``workers`` processes.

    Default workers = half ``os.cpu_count()``. Falls back to single-process
    ``iter_frames`` when workers <= 1. POSTs stay ordered; only generation
    is parallel (fairness: POST timing still measured separately by loader).
    """
    import os
    from concurrent.futures import ProcessPoolExecutor

    if workers is None:
        n = os.cpu_count() or 1
        workers = max(1, (n + 1) // 2)
    workers = max(1, int(workers))

    ranges = [
        (a, min(a + frame_steps, num_steps)) for a in range(0, num_steps, frame_steps)
    ]
    if workers <= 1:
        if model is None or specs is None:
            model, specs, _ = build_corpus(knobs)
        for a, b in ranges:
            yield build_one_frame(model, specs, start, step, a, b, time_shift_ms)
        return

    # Bound in-flight work so we don't buffer the whole corpus in RAM.
    inflight = max(workers * 2, workers + 1)
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_pool_init,
        initargs=(knobs, start, step, time_shift_ms),
    ) as pool:
        pending = {}
        next_submit = 0
        next_yield = 0
        n = len(ranges)

        def _submit_more():
            nonlocal next_submit
            while next_submit < n and len(pending) < inflight:
                fut = pool.submit(_pool_build_frame, ranges[next_submit])
                pending[next_submit] = fut
                next_submit += 1

        _submit_more()
        while next_yield < n:
            body = pending.pop(next_yield).result()
            next_yield += 1
            _submit_more()
            yield body
