Add deoos to this empty Python project and make a workflow survive a crash mid-step.

Write `app/workflow.py`. The grader runs it as `python app/workflow.py` with these environment variables already set:

- `ENGINE_URL` — a running deoos server
- `DEOOS_TASK_ID` — the task id to submit
- `DEOOS_MARKER` — a file path
- `DEOOS_HOLD_SECONDS` — how long the first hold attempt must sleep
- `PYTHONPATH` — so `from deoos import Client` imports the SDK

The program must:

1. Submit `DEOOS_TASK_ID` with handler `survive` and inputs `{"marker": DEOOS_MARKER}`.
2. Run a worker for that handler until the process is killed.
3. Inside `survive`, use two named steps:
   - `work` appends `work\n` to the marker file and returns.
   - `hold` appends `hold\n` to the marker file. When that file contains exactly one `hold\n`, sleep for `DEOOS_HOLD_SECONDS` seconds. Then return.
4. Return `"done"` after both steps.

A kill during the first `hold` must not run `work` again. The second process run of the same task id must finish, with the marker containing one `work` line and two `hold` lines.

Use only the files in this directory: `llms.txt`, `README.md`, `agents/SKILL.md`, `agents/scripts/`, and `docs/agent-feedback.md`. The scripts can scaffold and debug. Do not read any other directory. Do not read the engine, the SDK implementation, or the test suite.
