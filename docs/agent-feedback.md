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

1. If `GITHUB_TOKEN` or `GH_TOKEN` is set, the command creates a GitHub issue in `deoos-dev/deoos` and adds the `agent-feedback` label (creating that label when the token is allowed to).
2. Otherwise it prints a prefilled `https://github.com/deoos-dev/deoos/issues/new` URL and does not call GitHub. The title starts with `[agent-feedback]` and the body contains `<!-- deoos-agent-feedback -->`.

Opening that URL still requires a person or agent with permission to create the issue. The query string also asks GitHub for the `agent-feedback` label; GitHub applies it when the opener is allowed to set labels.

## Triage

[`.github/workflows/agent-feedback.yml`](../.github/workflows/agent-feedback.yml) runs when a new issue title starts with `[agent-feedback]` or the body contains `deoos-agent-feedback`. It creates the `agent-feedback` label if needed, adds the label, and comments with the checklist below.

The same form is in `.github/ISSUE_TEMPLATE/agent-feedback.yml` for a person filing by hand.

Group and fix:

1. Confirm the report has no secrets. If it does, delete the secret text before discussing it.
2. Filter issues with the `agent-feedback` label: `https://github.com/deoos-dev/deoos/issues?q=label%3Aagent-feedback`
3. Group issues that describe the same failure (same error text, same command, same status).
4. Fix that corner in the server, CLI, or SDK. Cover it with a behavioral test against a real engine.
5. Close the grouped issues with the commit or pull request link.
