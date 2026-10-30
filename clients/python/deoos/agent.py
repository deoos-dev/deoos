"""Compact CLI and MCP output when a coding agent is driving.

DEOOS_AGENT=1 or --agent turns it on. DEOOS_AGENT=0 turns it off even if
CURSOR_AGENT, CLAUDECODE, or CODEX is set. Output is one JSON line on stdout.
Progress is one stderr line. A capped list or history includes a notice that
says what was cut and how to get the rest.
"""
import json
import os
import sys

_DETECTORS = ("CURSOR_AGENT", "CLAUDECODE", "CODEX")
_OFF = ("", "0", "false", "no", "off")

LIST_NOTICE = (
    "showing the first 100 tasks; pass a known id to "
    "`python -m deoos inspect ID` or `python -m deoos summary ID` "
    "for a task that is not listed"
)
HISTORY_NOTICE = (
    "showing the last 32 retained events; older events are discarded "
    "and cannot be fetched"
)


def enabled(flag=False):
    if flag:
        return True
    raw = os.environ.get("DEOOS_AGENT")
    if raw is not None:
        return raw.strip().lower() not in _OFF
    return any(os.environ.get(name) not in (None, "", "0") for name in _DETECTORS)


def progress(action, detail=None, *, active):
    if not active:
        return
    if detail and action.startswith("task_"):
        print(f"deoos: {action} {detail}", file=sys.stderr)
    elif detail and action not in ("feedback", "list", "info"):
        print(f"deoos: {action} {detail}", file=sys.stderr)
    else:
        print(f"deoos: {action}", file=sys.stderr)


def dumps(value, active):
    if active:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False)


def annotate_list(result):
    if not result.get("truncated"):
        return result
    return {**result, "notice": LIST_NOTICE}


def history_body(events):
    """Agent-mode history. The cap is 32 stored events; there is no next page."""
    truncated = len(events) == 32
    body = {"events": events, "truncated": truncated}
    if truncated:
        body["notice"] = HISTORY_NOTICE
    return body
