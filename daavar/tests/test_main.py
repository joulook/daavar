"""The command line's contract (§8): one stdout line, exit 2 for any unexpected error, no bytecode writes."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import contextlib
import io
import shutil
import subprocess
import unittest
from unittest import mock

from daavar import __main__ as M
from daavar.tests.util import DAAVAR, FIXTURES, Scratch


def run_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = M.main(argv)
    return code, out.getvalue(), err.getvalue()


class Contract(unittest.TestCase):
    def test_unexpected_error_is_exit_2_without_traceback(self):
        """Review S1-S6 F4."""
        with mock.patch.object(M, "cmd_vocab", side_effect=RuntimeError("boom")):
            code, out, err = run_main(["vocab", str(FIXTURES / "vocab" / "fx")])
        self.assertEqual(code, 2)
        self.assertEqual(out, "daavar vocab: exit 2 REFUSED\n")
        self.assertIn("unexpected error: RuntimeError: boom", err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(len(err.strip().splitlines()), 1)
        with mock.patch.object(M, "cmd_vocab", side_effect=RuntimeError("two\nlines\x1b[31m")):
            code, out, err = run_main(["vocab", str(FIXTURES / "vocab" / "fx")])
        self.assertEqual(len(err.strip().splitlines()), 1)  # Astra round 3: exactly one line
        self.assertNotIn("\x1b", err)
        with mock.patch.object(M, "cmd_vocab", side_effect=RuntimeError("boom")):
            code, out, err = run_main(["vocab", str(FIXTURES / "vocab" / "fx"), "--debug"])
        self.assertIn("Traceback", err)

    def test_malformed_cargo_toml(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        p = s.path / "bad"
        (p / "rust" / "src").mkdir(parents=True)
        (p / "rust" / "Cargo.toml").write_text("[package\nname='x'\n")
        (p / "rust" / "Cargo.lock").write_text("")
        (s.path / "tf" / "traceforge").mkdir(parents=True)
        (s.path / "tf" / "traceforge" / "Cargo.toml").write_text("")
        (p / "daavar.toml").write_text('name = "x"\nmodel = "fifo"\n[programs.spec]\nkind = "spec"\nlang = "rust"\n'
                                       'side = "spec"\nargs = ["spec"]\n[scenarios.s]\nvisible = ["a"]\n'
                                       'programs = ["spec"]\ndirections = []\n[tools]\ntraceforge = "../tf"\n')
        code, out, err = run_main(["build", str(p)])
        self.assertEqual(code, 2)
        self.assertTrue(out.startswith("daavar build: exit 2 REFUSED"), out)
        self.assertNotIn("Traceback", err)

    def test_launcher_writes_no_bytecode(self):
        """Review S1-S6 A5/F6: bin/daavar runs with -B."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        d = s.path / "co"
        shutil.copytree(DAAVAR / "daavar", d / "daavar", ignore=shutil.ignore_patterns("__pycache__", "tests"))
        shutil.copytree(DAAVAR / "bin", d / "bin")
        r = subprocess.run([str(d / "bin" / "daavar"), "--version"], capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, timeout=60)
        self.assertEqual(r.stdout.strip(), "daavar 0.1.0")
        self.assertEqual(list((d / "daavar").rglob("__pycache__")), [])


if __name__ == "__main__":
    unittest.main()
