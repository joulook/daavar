"""The build cache (DRIVER-DESIGN §3.2): `~/.cache/daavar/` (`DAAVAR_CACHE` overrides it, D7).

  tools/<tool>-<identity>/       conf-join; JMC's core classes and cp.txt
  tools/jmc-<identity>.cli.json  the JMC CLI form `cli = "auto"` resolved to (S15)
  target/<tf-identity>/<key12>/  Cargo target directories (one per project path)
  artifacts/<build-key>/         a built artifact, read-only: build.json, out/ (the binary and its
                                 manifest, or the classes directory with SOURCE.md5), build.log
  locks/                         flock files; a directory is installed by renaming a finished
                                 temporary directory under its lock, so readers see all or nothing.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import stat
import tempfile
from pathlib import Path


def root() -> Path:
    env = os.environ.get("DAAVAR_CACHE")
    # Absolute: the root goes into children's arguments and environment (CARGO_TARGET_DIR), whose working
    # directories differ from the driver's (review S1-S6 A1).
    return Path(os.path.abspath(env)) if env else (Path.home() / ".cache" / "daavar").absolute()


def ensure(*parts: str) -> Path:
    p = root().joinpath(*parts)
    p.mkdir(parents=True, exist_ok=True)
    return p


@contextlib.contextmanager
def lock(name: str):
    """An exclusive flock on locks/<name>.lock for the block."""
    d = ensure("locks")
    path = d / (name.replace("/", "_") + ".lock")
    with open(path, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield path
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def tmpdir(under: Path) -> Path:
    under.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=".tmp-", dir=under))


def make_readonly(path: Path) -> None:
    """Every file and directory below `path` (and `path`) loses its write bits."""
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for f in filenames:
            p = os.path.join(dirpath, f)
            if not os.path.islink(p):
                m = os.stat(p).st_mode
                os.chmod(p, stat.S_IMODE(m) & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        for d in dirnames:
            p = os.path.join(dirpath, d)
            if not os.path.islink(p):
                os.chmod(p, 0o555)
    os.chmod(path, 0o555)


def remove(path: Path) -> None:
    """Removes a (possibly read-only) temporary tree."""
    if not os.path.lexists(path):
        return
    for dirpath, dirnames, _ in os.walk(path):
        for d in dirnames:
            p = os.path.join(dirpath, d)
            if not os.path.islink(p):
                os.chmod(p, 0o755)
    os.chmod(path, 0o755)
    shutil.rmtree(path)


def install(tmp: Path, final: Path) -> None:
    """Renames a finished temporary directory into place (same filesystem: atomic)."""
    os.rename(tmp, final)


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def key_of(obj) -> str:
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def tree_digest(d: Path) -> str:
    """sha256 over the sorted (path, sha256) pairs of the regular files below d."""
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(d):
        dirnames.sort()
        for f in sorted(filenames):
            p = Path(dirpath) / f
            if p.is_file() and not p.is_symlink():
                h.update(p.relative_to(d).as_posix().encode() + b"\0" + sha256_file(p).encode() + b"\n")
    return h.hexdigest()


def collect_stale_tmp(age_s: float = 86400.0) -> None:
    """Removes cache temporary directories older than a day (a driver killed mid-build or mid-export, Opus F15)."""
    import time
    now = time.time()
    for sub in ("exports", "artifacts", "tools"):
        d = root() / sub
        if not d.is_dir():
            continue
        for t in d.glob(".tmp-*"):
            try:
                if now - t.stat().st_mtime > age_s:
                    remove(t)
            except OSError:
                pass
