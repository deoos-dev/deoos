# deoos.dev site

- Edit copy in `src/index.md` (plus `src/llms.txt` and `src/deoos.md` for agents; keep all three in sync).
- If the opening or bullets change, update the root README and repository description as needed.
- Build: `python3 build.py` (writes `public/`).
- Show the diff and rendered text; wait for approval before deploying.
- Deploy after approval: `npx wrangler pages deploy public --project-name deoos`.
- Verify the new text at `https://deoos.dev` and `https://deoos.dev/llms.txt`, then commit and push as Derek Hecksher <74258642+hckshr@users.noreply.github.com>.
- `functions/api/signup.js` handles the hosted-DEOOS email signup (KV binding `SIGNUPS`). Don't change it unless asked.

Keep the page almost too simple: one opening paragraph, at most four bullets, "Try it" with only the GitHub link, then the hosted signup. No license text, version numbers, taglines, or unsupported claims.
