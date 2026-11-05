# Multi-arch handoff

Same six-step host maintenance run as the Python example. `os.machine()` is recorded on each step. Either worker can resume a task the other SDK started, against the same `deoos-server`.

## Run

```sh
python tests/multi_arch_handoff_demo.py
```

The arm64 phase of the reverse handoff runs this file.

## Expected output

```text
forward: killed python on worker-amd64 after backup
reverse: killed typescript on worker-arm64 after backup
multi-arch: audit matches the single-arch run; inventory and backup ran once
```
