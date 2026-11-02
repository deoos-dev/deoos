# Agent feedback

`python -m deoos feedback` and the MCP `feedback` tool report a sharp corner in deoos.

The report includes:

- deoos version
- what was tried
- what happened
- an optional repro, only when the caller sets it
- whether the engine responded, and its protocol version when it did

It does not read the workflow, the working tree, environment variables, or task payloads. Text that looks like a credential is refused unless the caller passes `--include-secrets` (CLI) or `include_secrets: true` (MCP).

## Transport

Filing is opt-in.

1. With no extra flag, the command is a dry run. It prints the exact issue payload (`title`, `body`, `labels`) and a prefilled `https://github.com/deoos-dev/deoos/issues/new` URL. It does not call GitHub. `--dry-run` is the same thing and wins over the opt-in below.
2. `--send`, `DEOOS_FEEDBACK=1`, or MCP `send: true` files the issue when `GITHUB_TOKEN` or `GH_TOKEN` is set. The command creates the `agent-feedback` label when the token is allowed to, then creates the issue in `deoos-dev/deoos` with that label.
3. If send was requested and no token is set, it prints the same payload and the prefilled URL and does not call GitHub.

The title starts with `[agent-feedback]` and the body contains `<!-- deoos-agent-feedback -->` plus a JSON object whose `kind` is `deoos-agent-feedback`. Opening the URL still requires a person or agent with permission to create the issue. The query string also asks GitHub for the `agent-feedback` label; GitHub applies it when the opener is allowed to set labels.

Text that matches a credential pattern, or that contains a secret-bearing environment value, is refused. The command prints the refusal and does not echo the secret. `engine_health` redacts the same patterns out of a storage error string before returning it.

## Triage

[`.github/workflows/agent-feedback.yml`](../.github/workflows/agent-feedback.yml) runs when a new issue title starts with `[agent-feedback]` or the body contains `deoos-agent-feedback`. It creates the `agent-feedback` label if needed, adds the label, and comments with the checklist below.

The same form is in `.github/ISSUE_TEMPLATE/agent-feedback.yml` for a person filing by hand.

Group and fix:

1. Confirm the report has no secrets. If it does, delete the secret text before discussing it.
2. Filter issues with the `agent-feedback` label: `https://github.com/deoos-dev/deoos/issues?q=label%3Aagent-feedback`
3. Group issues that describe the same failure. The JSON report in the body has `tried` and `happened`; the same `happened` text is the same corner.
4. Fix that corner in the server, CLI, or SDK. Cover it with a behavioral test against a real engine.
5. Close the grouped issues with the commit or pull request link.
