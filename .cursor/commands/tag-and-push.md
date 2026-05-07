---
description: Create annotated git tag and push it to origin
---

Run a release tag flow for this repository.

Use the user input after `/tag-and-push` as the version (for example: `v0.4.0`).

If no version is provided, ask for it before running commands.

Then execute this flow:

1. Show context:
   - `git log --oneline -10`
   - `git tag --sort=-version:refname | head -5`
2. Create an annotated tag using the provided version:
   - `git tag -a <VERSION> -m "$(cat <<'EOF'
<VERSION>

<Short summary line>

- Change 1
- Change 2
- Change 3
EOF
)"`
   - Replace placeholders using recent commits since the previous tag.
3. Push:
   - `git push origin <VERSION>`

Rules:
- Use annotated tags only (`-a`), never lightweight tags.
- Infer patch/minor/major from real changes.
- Never push anything except the requested tag.
