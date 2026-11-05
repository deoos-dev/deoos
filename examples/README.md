# deoos examples

Runnable examples for [deoos](https://github.com/deoos-dev/deoos) 0.8.0. The Python and Node SDKs are not on PyPI or npm. `pip install deoos` and `npm install deoos` do not install this project.

`scripts/install_deoos.sh` follows the install in the [deoos README](https://github.com/deoos-dev/deoos#install): download the release archive for this machine, check `SHA256SUMS`, then install that wheel, that Node tarball, and `deoos-server`. The Docker image does the same check. `deoos.lock` pins version `0.8.0`, git `6368fb380d422724ad1db40267abeebc68c94db0`.

Each example runs on real RustFS and the published `deoos-server`. Library hello is the exception: the same engine runs inside the process and still stores the step in RustFS. Nothing here mocks the engine. External HTTP targets in the payment, mail, agent, and integration demos are local stand-ins so the tests can crash a call on purpose.

## Setup

```sh
. ./scripts/prepare_local.sh
```

That installs deoos, starts RustFS on `127.0.0.1:19000`, creates the bucket `deoos-examples`, and exports the local storage variables. Leave that shell open.

`docker compose up -d` starts only RustFS. The demo services build `docker/Dockerfile` and run the checks under `tests/`. Results stay in `outputs/`.

## Examples

| Example | Need | Run |
| --- | --- | --- |
| [Library hello](python/library/README.md) | One durable greeting inside the process | `python python/library/hello.py` |
| [Server hello](python/server/README.md) | Same greeting through `deoos-server` | see the example README |
| [Order](python/order/README.md) | Child tasks, a timer, and an approval that survives a dead worker | `python python/order/order.py submit --id order-001` |
| [Hacker News](python/hacker-news/README.md) | A 100-story snapshot that resumes after 20 rows | `docker compose run --build --rm hacker-news` |
| [DEV articles](python/devto-etl/README.md) | A CSV that is not torn or fetched twice after a crash | `docker compose run --build --rm devto-etl` |
| [Payment](python/payment/README.md) | One charge across a crash and an expired idempotency window | `docker compose run --build --rm payment-resume` |
| [Document index](python/document-index/README.md) | An index that keeps documents already read | `docker compose run --build --rm document-index` |
| [Counting House](python/counting-house/README.md) | A refund a person approves, with compensation | `docker compose run --build --rm counting-house` |
| [HTTP integrations](python/integrations/README.md) | Webhook retry, invoice approval, scheduled import | `python tests/use_cases.py` |
| [Nightly batch](python/nightly-batch-resume/README.md) | 10,000 emails that do not resend after a kill | `docker compose run --build --rm nightly-batch-resume` |
| [Agent run](python/agent-run-resume/README.md) | Saved model calls and one charge key across a timeout | `docker compose run --build --rm agent-run-resume` |
| [Offline approval](python/offline-approval-ledger/README.md) | An air-gapped approval tied to one transfer | `./scripts/bundle_offline.sh` then `docker compose -f compose.offline.yaml run --rm offline-approval-ledger` |
| [Multi-arch handoff](python/multi-arch-handoff/README.md) | The same maintenance result on amd64 and arm64 | `python tests/multi_arch_handoff_demo.py` |

Python and TypeScript implement the same workflows, except Counting House, which is Python only. TypeScript commands are in each example README.

## Tests

`./scripts/test.sh` runs every example above against RustFS. GitHub Actions runs that script on pull requests and on pushes to `main`, then `docker compose run --build --rm counting-house`. The approval test builds `outputs/offline-bundle.tar` and runs it on an internal network. The handoff test builds the amd64 and arm64 workers and points both at `deoos-server`.

Start RustFS first (`docker compose up -d rustfs`, or the setup command above).
