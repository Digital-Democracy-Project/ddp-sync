# REPLY (SYNC-131): gates 1 and 2 FAIL on the EC2-broker host, so I stopped as the note says; nothing was changed (read-only, 2026-10-27 UTC)

Note `request-codebot-icon-on-ec2-sync131-20261027.md` (both of its commits) is **waiting for Ramon's go**. I have **not** done any step. The operator asked me to check the gates read-only and report; here they are.

## Gates
1. **`af6b098` is NOT in this host's `/opt/ddp-sync`: gate 1 FAILS.** Host `HEAD` is `74d72fe` ("Merge pull request #239 ... OPEN-330-daily-drift-check"); `origin/main` is `af6b098`; the host is **7 commits behind** (4 first-parent merges). `git merge-base --is-ancestor af6b098 HEAD` prints nothing and exits non-zero.
2. **Gate 2 FAILS:** `grep -c 'codebot_slack_icon_emoji' infrastructure/render-env.sh` prints **0** on this host (the host copy predates PR #243).
3. Quiet window (gate 3): not evaluated, since gate 1 already stops the procedure.

## What pulling main would deploy (why the note says not to pull)
The four first-parent merges between this host and `af6b098`: **PR #243 SYNC-131** (the render-env change this request is about), **PR #241 OPEN-420** (one display-name table), **PR #240 OPEN-418** (sync reads archive-scheduled), and **SYNC-122 / PR #242** (Voatz-to-Brevo jobs never add or re-import someone who opted out). So bringing this host to a commit that has the SYNC-131 change also deploys the other three; that is a bigger deploy than this note describes and needs its own decision.

## The secret write
`ddp-sync/credentials` has 36 keys today and **none** is `codebot_slack_icon_emoji` (read names only). This host's role is `EC2ServiceAccessReadOnlyRole`, so I cannot write the key; a person with write access has to add it (and the deploy above is needed before `render-env.sh` would copy it).

## What I need
Ramon's decision, in a commit naming this file: (a) deploy ddp-sync main (`af6b098`) on this host first, as its own staged request, then the secret key and a restart; or (b) wait. I will not pull, edit the secret or restart anything until then.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
