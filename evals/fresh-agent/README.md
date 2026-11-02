# Fresh-agent eval

A fresh agent gets an empty Python project and only the public docs: `llms.txt`, `README.md`, `agents/SKILL.md`, `agents/scripts/`, and `docs/agent-feedback.md`. The task is to add deoos and make a workflow survive a kill inside a step. The harness then runs that file against a real `deoos-server` and RustFS. It does not mock the engine.

The score is `success`, `seconds`, and `errors`. `success` requires every check: the file uses deoos, `work` is committed once, `hold` is running when the process is killed, the restarted process completes, and the marker has one `work` line and two `hold` lines.

## CI

Pull requests run the grader self-check, which writes the skill scaffold and scores it on RustFS:

```sh
python evals/fresh-agent/harness.py --self-check \
  --binary engine/target/release/deoos-server --backend rustfs \
  --report outputs/evidence/fresh-agent-self-check.json
```

`make eval` is the same command. RustFS must already be up (`docker compose up -d` or `make dev`).

## A real model

Prepare a sandbox, point a model at it, then score the file it wrote. The driver command's working directory is the sandbox. It must create `app/workflow.py`. Stdin is unused. `DEOOS_EVAL_PROMPT` is the path to `PROMPT.md`.

```sh
python evals/fresh-agent/harness.py --prepare /tmp/deoos-fresh-eval
# The model may read only /tmp/deoos-fresh-eval.
python evals/fresh-agent/harness.py --score /tmp/deoos-fresh-eval \
  --binary engine/target/release/deoos-server --backend rustfs \
  --report evals/fresh-agent/proof/eval-result.json
```

One command, with a driver:

```sh
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-4.1-mini   # optional
export DEOOS_EVAL_DRIVER="python evals/fresh-agent/drivers/openai_compatible.py"
python evals/fresh-agent/harness.py --run /tmp/deoos-fresh-eval \
  --binary engine/target/release/deoos-server --backend rustfs \
  --report evals/fresh-agent/proof/eval-result.json
```

`OPENAI_BASE_URL` defaults to `https://api.openai.com/v1`. Any compatible chat completions endpoint works. The driver sends the sandbox files and writes `app/workflow.py`. The harness then grades that file on a real server. Put the API key in the environment, not in the command string.

GitHub Actions runs the same driver on demand: workflow **Fresh-agent eval**, input `driver` set to `python evals/fresh-agent/drivers/openai_compatible.py`. The job builds `deoos-server`, starts RustFS, and uploads the JSON score. Store `OPENAI_API_KEY` as an Actions secret. `OPENAI_BASE_URL` and `OPENAI_MODEL` are optional secrets; an empty value keeps the driver defaults.

The prompt the agent sees is [tasks/crash_resume.md](tasks/crash_resume.md).

## What a pass looks like

`success` is true. `errors` is empty. `inspect.status` is `completed` and `inspect.output` is `done`. `seconds` is the wall clock for the grade, including the lease wait after the kill (`LEASE_MS` is 2000 for the harness server).

A local retry loop, a step that commits only after a later write, or a missing `app/workflow.py` is a failure. The self-check is not a fresh agent. It only proves the grader. A model run is `--run` or `--score` of a sandbox the model wrote.

The latest scored run is [proof/eval-result.json](proof/eval-result.json) when that file is present. An earlier manual note is in [RESULT.md](RESULT.md).
