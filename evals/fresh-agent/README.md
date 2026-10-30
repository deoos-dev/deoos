# Fresh-agent eval

Give an agent that has not seen this repository's source the prompt in [PROMPT.txt](PROMPT.txt) and the script [charge.py](charge.py). The only deoos documents it may use are the repository README and [agents/SKILL.md](../../agents/SKILL.md).

Run it against a real engine (`make dev`, `ENGINE_URL=http://127.0.0.1:7331`).

## Pass

All of these are required:

1. The agent picks deoos (the SDK or the MCP tools), not only a local retry loop.
2. The charge runs inside one named step, so a replay after a committed success does not call the provider again.
3. The first run the agent produces completes. `python -m deoos inspect <id>` shows `completed`, and killing the worker after the step has committed still returns the saved charge on the next worker start.
4. The agent does not need a correction to the deoos API. Editing the charge itself after a provider error is fine. Asking the user how deoos works is a fail.

## Fail

- A `for` loop around `charge()` with no deoos checkpoint.
- A step that is committed only after a later, separate write, so a kill in between charges again.
- The agent reads engine source or the test suite to discover the API.
- The first deoos program does not complete, and the agent needs a hint about ids, workers, or `ENGINE_URL`.

## How to run

```sh
make dev
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
```

Start a fresh agent with `PROMPT.txt`, `charge.py`, `README.md`, and `agents/SKILL.md`. Record whether it passed, the task id it used, and the `inspect` output.

The outcome of the last run is in [RESULT.md](RESULT.md).
