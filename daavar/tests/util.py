"""Shared test helpers: fixture paths and a scratch directory outside /tmp."""

import os
import shutil
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"
DAAVAR = HERE.parents[1]


def scratch_root() -> Path:
    """DAAVAR_TEST_TMP, else TMPDIR, else the tests' own .scratch directory (never /tmp by default)."""
    base = os.environ.get("DAAVAR_TEST_TMP") or os.environ.get("TMPDIR")
    if not base or base.startswith("/tmp"):
        base = str(HERE / ".scratch")
    Path(base).mkdir(parents=True, exist_ok=True)
    return Path(base)


# The cache the code under test may touch (runner.capture's temporary files) goes to the scratch root too,
# never to the user's ~/.cache, unless the caller set DAAVAR_CACHE.
os.environ.setdefault("DAAVAR_CACHE", str(scratch_root() / "daavar-test-cache"))


class Scratch:
    def __init__(self):
        self.path = Path(tempfile.mkdtemp(prefix="daavar-test-", dir=scratch_root()))

    def cleanup(self):
        def onerror(func, p, exc):
            os.chmod(os.path.dirname(p), 0o755)
            try:
                os.chmod(p, 0o755)
            except OSError:
                pass
            func(p)
        for root, dirs, files in os.walk(self.path):
            for d in dirs:
                try:
                    os.chmod(os.path.join(root, d), 0o755)
                except OSError:
                    pass
        shutil.rmtree(self.path, onerror=onerror)
