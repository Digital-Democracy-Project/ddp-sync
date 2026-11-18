"""SYNC-178: the `ddp-sync` command on a ddp-sync host (`scripts/ddp-sync`, linked into /usr/local/bin like `cams`).

    ddp-sync status           everything this host's ddp-sync is running, holding and has scheduled (plain text)
    ddp-sync status --json    the same as JSON (GET /ddp-sync/v1/status)

It asks the running service (DDP_SYNC_URL, default http://localhost:8001) with the service's own API key, read through
ddp-sync's settings (Secrets Manager, else .env) and never printed. Read only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "http://localhost:8001"
TIMEOUT_S = 20


def _api_key() -> str:
    from ddp_sync.config import get_settings

    return get_settings().api_key


def status(as_json: bool = False, url: str | None = None, api_key: str | None = None) -> int:
    base = (url or os.environ.get("DDP_SYNC_URL") or DEFAULT_URL).rstrip("/")
    request = urllib.request.Request(
        f"{base}/ddp-sync/v1/status" + ("" if as_json else "?format=text"),
        headers={"Authorization": f"Bearer {api_key if api_key is not None else _api_key()}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
            body = response.read().decode()
    except urllib.error.HTTPError as exc:
        reason = "the API key was refused" if exc.code == 401 else f"it answered {exc.code}"
        print(f"ddp-sync at {base}: {reason}.", file=sys.stderr)
        return 1
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"ddp-sync is not answering at {base} ({getattr(exc, 'reason', exc)}). Is the service running?",
              file=sys.stderr)
        return 1
    print(json.dumps(json.loads(body), indent=2) if as_json else body.rstrip("\n"))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ddp-sync", description="ddp-sync on this host.")
    sub = parser.add_subparsers(dest="command", required=True)
    status_parser = sub.add_parser("status", help="what ddp-sync is running, holding and has scheduled")
    status_parser.add_argument("--json", action="store_true", help="print the JSON answer instead of text")
    status_parser.add_argument("--url", help=f"the service's address (default DDP_SYNC_URL or {DEFAULT_URL})")
    args = parser.parse_args(argv)
    if args.command == "status":
        return status(as_json=args.json, url=args.url)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
