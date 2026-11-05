# Multi-arch handoff

A maintenance run has to keep going when the next worker is a different machine and a different SDK. The early steps stay saved. The audit result is the same whether the whole run stays on amd64 or moves to arm64.

The run has six steps over a local inventory: `inventory`, `backup`, `plan_updates`, `apply-host-1`, `apply-host-2`, and `audit`. `backup` writes one object to RustFS. Each step records the machine that ran it. The task result is the audit, which does not include the machine name.

## Run

From the repository root, with RustFS up. Register qemu when the host is not already the other architecture. The test builds amd64 and arm64 workers from the deoos 0.8.0 archive for that CPU, checks `SHA256SUMS`, and points both at one `deoos-server`:

```sh
python tests/multi_arch_handoff_demo.py
```

## Expected output

```text
forward: killed python on worker-amd64 after backup
reverse: killed typescript on worker-arm64 after backup
multi-arch: audit matches the single-arch run; inventory and backup ran once
```

The forward machine trace is `x86_64` for inventory and backup, then `aarch64` for the rest. Evidence stays in `outputs/multi-arch-handoff/`.
