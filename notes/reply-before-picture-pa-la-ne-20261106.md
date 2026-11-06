# Reply (read-only): before picture on RDS for pa, la, ne, and the revision question (OPEN-494, OPEN-478). 2026-11-06, about 20:50 UTC

Reply to 3a79efc. Read-only: SELECTs only, in a session set to read-only, run from the ddp-sync container with the live RDS credential (never printed). Nothing changed. Same join as the recompute baselines: `ddp_bill_version_document` to bill to legislative session to jurisdiction, `j.id like '%/state:<st>/%'`. Script and output are saved on this host (before_pic.py / before_pic.out in the job scratch).

## Items (a) to (c): `diff_from_previous_version`
| state | (a) rows with a non-null diff | (b) contain `<--` | (c) a line matching `^[+-]\s*Page [0-9]+ of [0-9]+\s*$` |
|---|---|---|---|
| pa | 921 | **909** (expected about 909 of 921: matches) | 0 |
| la | 4,541 | 0 | **853** (18.8%) |
| ne | 745 | 0 | 0 |

- The Louisiana footer rate is 853 of 4,541 (18.8%). You expected about 67 of every 300 (22%), so it is a bit lower than that; I did not look into why.
- Pattern detail: for (c) I used Postgres's newline-sensitive mode (`(?n)`), so `^` and `$` match at each line, with your pattern text unchanged. As written, `\s*` can also match a line break, so a footer followed by a blank line is counted the same as one at the end. If you meant a stricter pattern, tell me and I will rerun it.
- (b) is a plain substring search for `<--`.

## Item (d): Pennsylvania `motion_classification` on `opencivicdata_voteevent`
Joined by vote event to legislative session to jurisdiction (`pa`). 5,858 vote events in total, which equals your first-scrape count of 5,858:
| value | count |
|---|---|
| `{}` (empty) | 4,873 |
| `{passage}` | 953 |
| `{amendment}` | 32 |

- **Not empty and not `amendment`/`reading-3`: 953 events, all `{passage}`.** There are no `reading-3` values at all. So the old behaviour is not "empty or amendment": v43 already sets `passage` on 953 of them. Note this is for the whole table of pa vote events, not only the first scrape.

## Item 3: live task-definition revision
**Cannot.** This host's role (`EC2ServiceAccessReadOnlyRole`) is denied `ecs:DescribeTaskDefinition`, `ecs:ListTaskDefinitions` and `ecr:DescribeImages`. It can read ECS `list-tasks` and CloudWatch logs, but that does not say which revision ran. The last revision I saw used was v43 / revision 46 (11-06 03:17 UTC). Which account or tool can read it: I do not know of one on this host; the Mac side, Ramon's own AWS login, or a role with those three actions would.

## Item 1 (tonight's run)
Yes: the watcher wakes at 04:03 UTC. I will post at launch, at about 3 hours, and at the end, and will not retry if it fails.
