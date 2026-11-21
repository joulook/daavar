"""S1: the runner kills the whole group on a timeout, classes the timeout, closes stdin."""

import os
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar
import signal
import time
import unittest
from pathlib import Path

from daavar import runner
from daavar.tests.util import Scratch

PY = sys.executable


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:  # a zombie that init has not reaped yet counts as dead
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, IndexError):
        return False


def wait_dead(pid: int, s: float = 3.0) -> bool:
    end = time.monotonic() + s
    while time.monotonic() < end:
        if not alive(pid):
            return True
        time.sleep(0.02)
    return not alive(pid)


# A child that starts a grandchild (writing both pids to a file), then sleeps.
PARENT = r"""
import os, subprocess, sys, time, signal
if sys.argv[2] == "ignore":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
g = subprocess.Popen([sys.executable, "-c",
    "import signal, time, sys\n"
    "if sys.argv[1] == 'ignore': signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "time.sleep(60)", sys.argv[2]])
with open(sys.argv[1], "w") as f:
    f.write(f"{os.getpid()} {g.pid}\n")
if sys.argv[3] == "exit":
    sys.exit(3)
time.sleep(60)
"""


# A parent dying on SIGTERM whose child ignores SIGTERM and closed stdout/stderr.
PARENT_DETACHED = r"""
import os, subprocess, sys, time
g = subprocess.Popen([sys.executable, "-c",
    "import os, signal, time\n"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "os.close(1); os.close(2)\n"
    "time.sleep(60)"])
with open(sys.argv[1], "w") as f:
    f.write(f"{os.getpid()} {g.pid}\n")
time.sleep(60)
"""


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.d = self.s.path

    def tearDown(self):
        self.s.cleanup()

    def pids(self):
        for _ in range(200):
            p = self.d / "pids"
            if p.exists() and p.read_text().endswith("\n"):
                return [int(x) for x in p.read_text().split()]
            time.sleep(0.02)
        self.fail("no pids written")

    def test_exit_code_streams_and_stdin_closed(self):
        r = runner.run([PY, "-c", "import sys; d = sys.stdin.read(); print('in', len(d)); "
                        "print('err', file=sys.stderr); sys.exit(7)"],
                       self.d, self.d / "o", self.d / "e", timeout_s=20)
        self.assertEqual(r.exit, 7)
        self.assertFalse(r.timed_out)
        self.assertEqual(r.read_stdout(), "in 0\n")
        self.assertEqual(r.read_stderr(), "err\n")
        self.assertIsNotNone(r.peak_rss_kb)
        self.assertGreater(r.peak_rss_kb, 0)

    def test_env_delta(self):
        r = runner.run([PY, "-c", "import os; print(os.environ.get('DAAVAR_X'), os.environ.get('DAAVAR_Y'))"],
                       self.d, self.d / "o", self.d / "e", env_delta={"DAAVAR_X": "1", "DAAVAR_Y": None},
                       base_env={**os.environ, "DAAVAR_Y": "gone"})
        self.assertEqual(r.read_stdout(), "1 None\n")
        self.assertEqual(r.env_delta, {"DAAVAR_X": "1", "DAAVAR_Y": None})

    def test_timeout_kills_grandchild(self):
        r = runner.run([PY, "-c", PARENT, str(self.d / "pids"), "term", "sleep"], self.d, self.d / "o",
                       self.d / "e", timeout_s=1.0, kill_grace_s=5.0)
        child, grand = self.pids()
        self.assertTrue(r.timed_out)
        self.assertFalse(r.killed)  # SIGTERM sufficed
        self.assertEqual(r.exit, -15)
        self.assertTrue(wait_dead(child))
        self.assertTrue(wait_dead(grand), "the grandchild outlived the timeout")
        self.assertLess(r.wall_ms, 4000)

    def test_timeout_sigkill_after_grace(self):
        r = runner.run([PY, "-c", PARENT, str(self.d / "pids"), "ignore", "sleep"], self.d, self.d / "o",
                       self.d / "e", timeout_s=1.0, kill_grace_s=0.5)
        child, grand = self.pids()
        self.assertTrue(r.timed_out)
        self.assertTrue(r.killed)
        self.assertEqual(r.exit, -9)
        # SIGKILL came only after the grace: timeout 1 s + grace 0.5 s.
        self.assertGreaterEqual(r.wall_ms, 1450)
        self.assertLess(r.wall_ms, 4000)
        self.assertTrue(wait_dead(child))
        self.assertTrue(wait_dead(grand), "the grandchild ignoring SIGTERM survived SIGKILL")

    def test_leftover_grandchild_killed_after_exit(self):
        r = runner.run([PY, "-c", PARENT, str(self.d / "pids"), "term", "exit"], self.d, self.d / "o",
                       self.d / "e", timeout_s=30)
        child, grand = self.pids()
        self.assertEqual(r.exit, 3)
        self.assertFalse(r.timed_out)
        self.assertTrue(r.leftovers_killed)
        self.assertTrue(wait_dead(grand))

    def test_grace_respected_when_sigterm_is_ignored(self):
        r = runner.run([PY, "-c", PARENT, str(self.d / "pids"), "ignore", "sleep"], self.d, self.d / "o",
                       self.d / "e", timeout_s=0.5, kill_grace_s=2.0)
        self.assertTrue(r.killed)
        self.assertGreaterEqual(r.wall_ms, 2450)

    def test_capture_waits_the_grace_for_a_surviving_descendant(self):
        """The parent exits on SIGTERM; its child ignores SIGTERM and has closed its output: SIGKILL only
        after the grace."""
        t0 = time.monotonic()
        code, out, err = runner.capture([PY, "-c", PARENT_DETACHED, str(self.d / "pids")], timeout_s=0.5,
                                        kill_grace_s=1.5)
        took = time.monotonic() - t0
        child, grand = self.pids()
        self.assertEqual(code, 124)
        self.assertGreaterEqual(took, 1.9)
        self.assertTrue(wait_dead(grand))

    def test_capture_timeout_kills_the_group(self):
        code, out, err = runner.capture([PY, "-c", PARENT, str(self.d / "pids"), "term", "sleep"], timeout_s=1.0)
        child, grand = self.pids()
        self.assertEqual(code, 124)
        self.assertTrue(wait_dead(child))
        self.assertTrue(wait_dead(grand), "capture left the grandchild running")

    def interrupted_run(self, sig, handler=None):
        import signal as S
        import threading
        prev = S.signal(sig, handler) if handler else None
        try:
            threading.Timer(1.0, lambda: os.kill(os.getpid(), sig)).start()
            with self.assertRaises(BaseException) as cm:
                runner.run([PY, "-c", PARENT, str(self.d / "pids"), "term", "sleep"], self.d, self.d / "o",
                           self.d / "e", timeout_s=60, kill_grace_s=2.0)
        finally:
            if prev is not None:
                S.signal(sig, prev)
        child, grand = self.pids()
        self.assertTrue(wait_dead(child), "the child outlived the driver's interruption")
        self.assertTrue(wait_dead(grand), "the grandchild outlived the driver's interruption")
        return cm.exception

    def test_sigint_stops_the_group(self):
        """Review S1-S6 F1: Ctrl-C reaches only the driver; the driver stops the child's group."""
        self.assertIsInstance(self.interrupted_run(signal.SIGINT), KeyboardInterrupt)

    def test_sigterm_stops_the_group(self):
        from daavar import __main__ as M
        self.assertIsInstance(self.interrupted_run(signal.SIGTERM, M._on_sigterm), runner.Interrupted)

    def test_interrupt_during_cleanup_still_kills(self):
        """Astra round 3: a second interrupt during the timeout cleanup cannot leave descendants running."""
        import threading
        threading.Timer(1.0, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
        with self.assertRaises(KeyboardInterrupt):
            # the leader dies on SIGTERM at 0.5 s; its child ignores SIGTERM: the cleanup waits the grace (2 s)
            runner.run([PY, "-c", PARENT_DETACHED, str(self.d / "pids")], self.d, self.d / "o", self.d / "e",
                       timeout_s=0.5, kill_grace_s=2.0)
        child, grand = self.pids()
        self.assertTrue(wait_dead(grand), "the descendant outlived an interrupted cleanup")

    def test_cannot_start(self):
        r = runner.run([str(self.d / "nope")], self.d, self.d / "o", self.d / "e")
        self.assertEqual(r.exit, 127)
        self.assertIn("cannot start", r.read_stderr())

    def test_record(self):
        r = runner.run([PY, "-c", "pass"], self.d, self.d / "o", self.d / "e")
        rec = r.as_record()
        self.assertEqual(rec["exit"], 0)
        self.assertEqual(rec["argv"][0], PY)
        self.assertEqual(Path(rec["cwd"]), self.d)


if __name__ == "__main__":
    unittest.main()
