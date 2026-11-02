# Fresh-agent result

The scored runs are in [proof/](proof/). Both used a real `deoos-server` and RustFS. The agent was a fresh model with a sandbox that contained only public docs. It was told not to read the repository.

| Run | What the agent had | Result |
| --- | --- | --- |
| [proof/eval-prose.json](proof/eval-prose.json) | `llms.txt`, README, and the skill. No scripts. The agent wrote [proof/prose-workflow.py](proof/prose-workflow.py). | `success: true` in 2.443s. `errors` is empty. Task `crash-995a8d59561b` completed with output `done` after a kill during `hold`. The marker had one `work` line and two `hold` lines. |
| [proof/eval-result.json](proof/eval-result.json) | The same docs plus `agents/scripts`. The agent ran `scaffold_workflow.py`. | `success: true` in 2.449s. Same crash-resume checks. |

[proof/self-check.json](proof/self-check.json) is the CI grader, not a fresh agent. It runs the skill scaffold through the same checks. [proof/mcp-workflow.txt](proof/mcp-workflow.txt) is the MCP server driving that workflow. [proof/feedback-dry-run.json](proof/feedback-dry-run.json) is the issue payload. Nothing was filed. `gh` in this environment cannot create issues.

An earlier manual charge note from 2026-10-30 described a script written against the skill before this harness existed. The JSON files above are the runs to trust.
