"""Run directories (DRIVER-DESIGN §5): one atomic `out/<UTC time>-<scenario>-<pid>-<rand6>` per check,
`out/latest` replaced atomically, `out/.lock` (flock) held for the whole check."""

from __future__ import annotations

import contextlib
import fcntl
import os
import secrets
import time
from pathlib import Path

from . import DaavarError


class Busy(DaavarError):
    pass


@contextlib.contextmanager
def lock(out: Path, wait: bool = True):
    """The check lock. A concurrent check queues; with wait False it is refused (Busy)."""
    out.mkdir(parents=True, exist_ok=True)
    f = open(out / ".lock", "a+")
    try:
        flags = fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB)
        try:
            fcntl.flock(f.fileno(), flags)
        except BlockingIOError:
            raise Busy(f"{out}/.lock is held by another check (--no-wait)") from None
        yield
    finally:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        finally:
            f.close()


def allocate(out: Path, scenario: str) -> Path:
    """A fresh run directory, created atomically (mkdir fails on a name clash, then another name is drawn)."""
    out.mkdir(parents=True, exist_ok=True)
    for _ in range(100):
        name = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{scenario}-{os.getpid()}-{secrets.token_hex(3)}"
        d = out / name
        try:
            d.mkdir()
            return d
        except FileExistsError:
            continue
    raise DaavarError(f"cannot allocate a run directory under {out}")


def set_latest(out: Path, run: Path) -> None:
    tmp = out / f".latest-{os.getpid()}-{secrets.token_hex(3)}"
    os.symlink(run.name, tmp)
    os.replace(tmp, out / "latest")
