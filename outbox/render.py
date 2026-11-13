#!/usr/bin/env python3
"""Rebuild outbox/README.md from outbox/index.json.

README.md is generated: never edit it by hand. set_status.py calls this after every change. Run it
yourself after editing index.json directly (a new item, a new summary or decision); it also bumps
index.json's "updated".

Usage: python3 outbox/render.py

The README's first section is James's to-do list, in order:
  1. sync review fixes (fixes-ready): an upstream reviewer is waiting
  2. submit (ready-to-submit)
  3. decide (any open item with a "decision"; a decision on a sync or submit item comes just before that move)
  4. build and A/B (ready-to-build)
Ties go by "priority", lower first. Items with no move for James are listed under "With lil red",
submitted ones under "Submitted", dropped ones under "Dropped".
"""
import datetime
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent
INDEX_PATH, README_PATH = ROOT / "index.json", ROOT / "README.md"

# status -> (rank, the move, the lil-red-outbox skill command)
JAMES_MOVES = {
    "fixes-ready": (1, "Sync review fixes", "sync"),
    "ready-to-submit": (2, "Submit", "submit"),
    "ready-to-build": (4, "Build and A/B", "ab"),
}
DECIDE_RANK = 3
LILRED_DOING = {
    "results-in": "judging your results",
    "needs-work": "reworking the branch, or dropping it",
}
DONE = ("submitted", "dropped")


def load():
    return json.loads(INDEX_PATH.read_text())


def _cell(text):
    return " ".join(str(text).split()).replace("|", "\\|")


def _summary(item):
    if item.get("summary"):
        return _cell(item["summary"])
    claim = _cell(item.get("claim", ""))
    first = claim.split(". ")[0]
    return first if len(first) <= 200 else first[:197] + "..."


def _item_link(item):
    return f"[{item['slug']}](../prs/{item['slug']}.md)" if item.get("pr_text") else item["slug"]


def _issue_link(index, item):
    n = item.get("issue")
    return f"[#{n}](https://github.com/{index['repo']}/issues/{n})" if n else ""


def render(index):
    items = sorted(index["items"], key=lambda i: (i.get("priority", 999), i["slug"]))
    for item in items:
        if item["status"] not in index["statuses"]:
            raise SystemExit(f"{item['slug']}: unknown status {item['status']!r}")

    todo = []  # (rank, priority, sub, cells)
    with_lilred, submitted, dropped = [], [], []
    for item in items:
        status, prio = item["status"], item.get("priority", 999)
        decision = item.get("decision") if status not in DONE else None
        move = JAMES_MOVES.get(status)
        if decision:
            rank = min(DECIDE_RANK, move[0]) if move else DECIDE_RANK
            todo.append((rank, prio, 0, ["**Decide**", _item_link(item), _issue_link(index, item), _cell(decision)]))
        if move:
            rank, label, command = move
            todo.append((rank, prio, 1, [f"**{label}**: `{command} {item['slug']}`", _item_link(item),
                                         _issue_link(index, item), _summary(item)]))
        elif status == "submitted":
            pr = item.get("upstream_pr")
            submitted.append([_item_link(item), f"PR {pr}" if pr else "", _issue_link(index, item), _summary(item)])
        elif status == "dropped":
            dropped.append([_item_link(item), _issue_link(index, item), _summary(item)])
        elif not decision:
            with_lilred.append([_item_link(item), _issue_link(index, item),
                                LILRED_DOING.get(status, status), _summary(item)])
    todo.sort(key=lambda t: t[:3])

    def table(header, rows):
        out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        return out + ["| " + " | ".join(r) + " |" for r in rows]

    updated = index.get("updated", "")
    try:
        updated = datetime.datetime.strptime(updated, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        pass

    lines = [
        "# lil red's outbox",
        "",
        "What James does for lil red, in order. When a move is done, its row leaves this list: a submitted item "
        "moves to **Submitted**, and an item lil red is working on moves to **With lil red**.",
        "",
        f"Generated from [`index.json`](index.json) by `render.py`; don't edit this file by hand. Updated {updated}.",
        "",
        "## For you, in order",
        "",
    ]
    if todo:
        lines += table(["#", "your move", "item", "issue", "what it's for"],
                       [[str(n)] + t[3] for n, t in enumerate(todo, 1)])
        lines += ["", "Order: review fixes to sync first (an upstream reviewer is waiting), then submits, then decisions, "
                  "then builds. Within each, lil red's `priority` order. A move is a command for the `lil-red-outbox` "
                  "skill; answer a decision on the item's issue."]
    else:
        lines.append("Nothing for you right now.")
    lines += ["", "## With lil red: nothing for you", ""]
    lines += table(["item", "issue", "lil red is", "what it's for"], with_lilred) if with_lilred else ["None."]
    lines += ["", "## Submitted", ""]
    lines += table(["item", "upstream", "issue", "what it does"], submitted) if submitted else ["None yet."]
    if dropped:
        lines += ["", "## Dropped", ""] + table(["item", "issue", "what it was"], dropped)
    lines += [
        "",
        "## How an item moves",
        "",
        "1. `ready-to-build`: James's Cursor skill builds the item's `base` and `head` commits, runs the listed tests "
        "and the A/B, and posts the results on the item's issue.",
        "2. `results-in`: lil red checks the results against `pass_if` and writes the real numbers into the PR text. "
        "It drops `[unbuilt]` and runs its review-bot preflight again.",
        "3. `ready-to-submit`: James's skill submits it. That means a branch `jtcunning-<slug>` and an upstream PR "
        "whose body is the text between the `<!-- upstream:start -->` and `<!-- upstream:end -->` markers of "
        "`prs/<slug>.md`.",
        "   - `needs-work`: lil red is reworking or dropping the item instead.",
        "4. `submitted`: done. The skill's `<!-- lil-red-outbox:submitted -->` comment sets it. lil red also sets it "
        "on its own when it finds an upstream PR from `jtcunning-<slug>`, or a PR that James linked on the item's "
        "issue; it checks every day at 02:00 PT, and at the start of each 03:00 and 15:00 PT work run. If review "
        "feedback comes in, the skill posts it on the issue. lil red pushes fixes to its branch (`fixes-ready`), and "
        "the skill syncs them into `jtcunning-<slug>`.",
        "",
        "A **decision** (the item's `decision` field) is a question for James that lil red asked on the item's issue. "
        "It stays in the list until lil red reads his answer.",
        "",
        "Statuses:",
        "",
    ]
    lines += [f"- `{name}`: {text}" for name, text in index["statuses"].items()]
    lines += [
        "",
        "Only lil red edits this directory. James's skill reports through comments on the item's issue. Each comment "
        "starts with a marker that lil red's intake reads: `<!-- lil-red-outbox:results -->`, "
        "`<!-- lil-red-outbox:submitted -->` or `<!-- lil-red-outbox:feedback -->`.",
        "",
    ]
    return "\n".join(lines)


def save(index):
    """Bump "updated", write index.json, and rebuild README.md from it."""
    index["updated"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    readme = render(index)  # validate before writing anything
    INDEX_PATH.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")
    README_PATH.write_text(readme)


if __name__ == "__main__":
    save(load())
    print("README.md rebuilt from index.json")
