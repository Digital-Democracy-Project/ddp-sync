"""SYNC-137: an alert sent from an async job must not freeze the event loop.

`post_alert` is blocking: with the CAMS route unreachable it waits up to 5 s for the route, then up to 15 s for
Slack. Called straight from an `async def`, that stalls the whole service (every scheduled job, the API)
for the time of the call. Async callers send it with `asyncio.to_thread`, the way the rest of this package
runs blocking work. These tests fail if one forgets.
"""

from __future__ import annotations

import ast
import asyncio
import stat
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from ddp_sync.pipelines.docker_cleanup import run_docker_cleanup_job

_SRC = Path(__file__).parent.parent / "src"


def _functions(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _own_calls(func: ast.AST):
    """Calls made by `func` itself, not by a function defined inside it."""
    stack = list(ast.iter_child_nodes(func))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            yield node.func.id
        stack.extend(ast.iter_child_nodes(node))


def test_no_async_function_calls_a_blocking_alert_directly():
    """`post_alert`, and every plain function in the package that calls it (`_alert_scrape_failure`,
    `_alert_archive_failure`, ...), is blocking. An `async def` must hand it to `asyncio.to_thread`
    (which takes the function as an argument, so it is not a call here). The scan follows one hop (a plain function
    that calls `post_alert` directly) and plain names only: it guards today's call shapes, not every way a
    future one could be written."""
    trees = {path: ast.parse(path.read_text()) for path in (_SRC / "ddp_sync").rglob("*.py")}
    blocking = {"post_alert"}
    for tree in trees.values():
        for func in _functions(tree):
            if isinstance(func, ast.FunctionDef) and "post_alert" in set(_own_calls(func)):
                blocking.add(func.name)
    offenders = sorted(
        f"{path.relative_to(_SRC)}:{func.name}"
        for path, tree in trees.items()
        for func in _functions(tree)
        if isinstance(func, ast.AsyncFunctionDef) and blocking & set(_own_calls(func))
    )
    assert not offenders, f"send the alert with `await asyncio.to_thread(<function>, ...)`: {offenders}"


@pytest.mark.asyncio
async def test_a_slow_alert_does_not_stall_the_event_loop(tmp_path):
    """A failing Docker cleanup alerts; make that alert take 0.6 s and watch a ticker running beside it."""
    script = tmp_path / "docker-prune.sh"
    script.write_text("#!/bin/bash\necho 'docker unreachable' >&2; exit 1\n")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)

    def slow_alert(*_args, **_kwargs):
        time.sleep(0.6)
        return True

    gaps: list[float] = []
    stop = asyncio.Event()

    async def ticker():
        last = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(0.02)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    with patch("ddp_sync.pipelines.docker_cleanup.post_alert", side_effect=slow_alert) as alert:
        tick = asyncio.create_task(ticker())
        result = await run_docker_cleanup_job(str(script))
        stop.set()
        await tick

    alert.assert_called_once()
    assert result["success"] is False
    assert max(gaps) < 0.3, f"the event loop stalled for {max(gaps):.2f}s while the alert was being sent"
