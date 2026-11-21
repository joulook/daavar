"""Regression tests of the S7-S10 reviews (Astra driver-s710-review-01, Opus REVIEW-driver-s710-opus)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import copy
import io
import contextlib
import json
import os
import shutil
import subprocess
import tomllib
import unittest
from unittest import mock

from daavar import DaavarError, build, check, config, outcome, provenance, report, verdict
from daavar import __main__ as M
from daavar.outcome import Outcome
from daavar.parse import ExportFile, JoinReport
from daavar.tests.util import FIXTURES, Scratch
from daavar.tests.test_outcome import cls

D = verdict.direction


def exp(enum="complete"):
    return Outcome("ok", "", data={"export": ExportFile("x", {"enumeration": enum}, 3), "stopped_early": False})


def join(unc, viol):
    rep = JoinReport({}, {}, "violation" if unc else "conforms", "", unc, None,
                     {"uncovered": unc, "violations": viol, "false_alarms": 0, "unresolved": unc - viol}, [])
    return Outcome("ok", "", data={"report": rep})


class Verdicts(unittest.TestCase):
    def test_both_sides_classified_paired_tools(self):
        """Astra 3 / Opus F1: the four rows, with the Rust and the JMC fixtures of the same situation."""
        for ref_to, chk_fail, chk_ref in (
                (cls("tf", "export", "tf-export-sleep"), cls("tf", "export", "tf-export-impl-panic"),
                 cls("tf", "export", "tf-export-usage")),
                (cls("jmc", "export", "jmc-export-sleep"), cls("jmc", "export", "jmc-export-assert-spawned"),
                 cls("jmc", "export", "jmc-export-badargs"))):
            self.assertEqual(D(ref_to, chk_fail, None).status, "violation")
            self.assertEqual(D(ref_to, chk_ref, None).status, "refused")
            self.assertEqual(D(exp("aborted"), chk_fail, None).status, "violation")
            self.assertEqual(D(exp("aborted"), chk_ref, None).status, "refused")
        self.assertEqual(D(cls("tf", "export", "tf-export-badvis"), cls("tf", "export", "tf-export-impl-panic"),
                           None).status, "refused")  # the reference wins a tie of worse-or-equal

    def test_incomplete_reference_without_online_export(self):
        """Astra 1 / Opus F11: the online export is skipped; the direction is incomplete."""
        v = D(exp("aborted"), None, None)
        self.assertEqual((v.status, v.reason), ("incomplete", "ref-incomplete"))

    def test_join_timeout_incomplete_and_violation_elsewhere_wins(self):
        """Astra 2."""
        v = D(exp(), exp(), Outcome("timeout", "t", "budgets", "timeout"))
        self.assertEqual(v.status, "incomplete")
        d1, d2 = config.Direction("a", "s", "impl<=spec", True), config.Direction("b", "s", "impl<=spec", True)
        a = verdict.aggregate([("c", [(d1, v), (d2, D(exp(), exp(), join(1, 1)))])])
        self.assertEqual((a.status, a.exit), ("VIOLATION", 1))


class Cache(unittest.TestCase):
    def test_jmc_timeout_not_cacheable(self):
        """Opus F2."""
        o = Outcome("ok", "", data={"measurements": {"stop_reason": "timeout"}})
        self.assertFalse(check.cacheable(o))
        self.assertTrue(check.cacheable(Outcome("ok", "", data={"measurements": {"stop_reason": "exhausted"}})))
        self.assertFalse(check.cacheable(Outcome("failure", "x")))

    def test_budgets_in_the_key_and_mem_cap_refused(self):
        """Astra 8."""
        cfg = config.load(FIXTURES / "config" / "a2a-task.daavar.toml")
        c = check.Check(cfg, check.Options())

        class B:
            tools = {"traceforge": {"identity": "t"}, "jmc": {"identity": "j", "cli": "positional", "classpath": [],
                                                             "main": "M"}}
            artifacts = {"rust": {"key": "k", "manifest": "m", "bins": {"b": "/b"}},
                         "java": {"key": "k2", "manifest": "m2", "classes": "/c"}}
        c.build, c.rust_bin = B, "b"
        (cell,) = config.select(cfg, scenarios=["one"], cells=["a"])
        k1 = c.key_of(cell.invocations["spec"], "offline", None)[0]
        kj = c.key_of(cell.invocations["java"], "offline", None)[2]
        self.assertEqual((kj["header"]["program"], kj["header"]["size"]), ("a2a-task", "impl,a"))
        cfg.budgets["join_timeout_s"] = 1
        self.assertNotEqual(c.key_of(cell.invocations["spec"], "offline", None)[0], k1)
        cfg.budgets["mem_cap_gb"] = 1
        with self.assertRaisesRegex(DaavarError, "mem_cap_gb"):
            check.Check(cfg, check.Options())

    def test_java_timeout_zero_refused(self):
        """Opus F6."""
        r = tomllib.loads((FIXTURES / "config" / "a2a-task.daavar.toml").read_text())
        r["budgets"]["export_timeout_s"] = 0
        with self.assertRaisesRegex(config.ConfigError, "at least 1 with Java"):
            config.from_dict(r, FIXTURES / "config")


class Restore(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()

    def tearDown(self):
        self.s.cleanup()

    def test_absolute_paths_rebased(self):
        """Astra 5 / Opus F4: restore into another directory; absolute [tools] and path dependency rewritten."""
        old = self.s.path / "orig"
        p = self.s.path / "D" / "proj"
        (p / "rust").mkdir(parents=True)
        (p / "daavar.toml").write_text(f'name = "x"\n[tools]\ntraceforge = "{old}/tf"\n[tools.jmc]\n'
                                       f'checkout = "{old}/jmc"\n')
        (p / "rust" / "Cargo.toml").write_text(f'[dependencies]\ntraceforge = {{ path = "{old}/tf/traceforge" }}\n')
        places = {"traceforge": (old / "tf", self.s.path / "D" / "tf"), "jmc": (old / "jmc", self.s.path / "D" / "jmc"),
                  "project": (old / "proj", p)}
        out = provenance.rebase(p, places, lambda m: None)
        self.assertEqual(len(out), 3)
        t = tomllib.loads((p / "daavar.toml").read_text())
        self.assertEqual(t["tools"]["traceforge"], str(self.s.path / "D" / "tf"))
        self.assertEqual(t["tools"]["jmc"]["checkout"], str(self.s.path / "D" / "jmc"))
        c = tomllib.loads((p / "rust" / "Cargo.toml").read_text())
        self.assertEqual(c["dependencies"]["traceforge"]["path"], str(self.s.path / "D" / "tf" / "traceforge"))

    def test_artifacts_binding(self):
        """Astra 4 / Opus F12: only a Rust binary rebuilt elsewhere from the same key may differ."""
        prov = {"artifacts": {"rust": {"key": "k", "dir": "/a", "sha256": {"b": "1", "b.SOURCE.md5": "m"}},
                              "java": {"key": "j", "dir": "/j", "sha256": "J"}},
                "tools": {"traceforge": {"identity": "t", "conf_join_sha256": "c"},
                          "jmc": {"identity": "i", "classes_sha256": "x"}}}
        n = copy.deepcopy(prov)
        n["artifacts"]["rust"].update(dir="/b", sha256={"b": "2", "b.SOURCE.md5": "m"})
        res = {x["name"]: x for x in provenance.verify_artifacts(prov, n, [])}
        self.assertTrue(res["rust:b"]["allowed"])
        self.assertTrue(res["rust:b.SOURCE.md5"]["equal"])
        n["artifacts"]["java"]["sha256"] = "J2"
        n["tools"]["jmc"]["classes_sha256"] = "y"
        res = {x["name"]: x for x in provenance.verify_artifacts(prov, n, [])}
        self.assertFalse(res["java:java"]["equal"] or res["java:java"]["allowed"])
        self.assertFalse(res["tool:jmc:classes"]["allowed"])
        n2 = copy.deepcopy(prov)
        n2["artifacts"]["rust"]["sha256"] = {"b": "2", "b.SOURCE.md5": "m"}  # same dir, different bytes
        self.assertFalse({x["name"]: x for x in provenance.verify_artifacts(prov, n2, [])}["rust:b"]["allowed"])

    def test_snapshot_identity(self):
        """Astra 6: a snapshot-restored tool keeps its recorded identity."""
        d = self.s.path / "tf"
        d.mkdir()
        (d / build.IDENTITY_MARKER).write_text(json.dumps({"tool": "traceforge", "commit": "c" * 40,
                                                           "dirty_digest": "d" * 64}))
        t = build.tool_identity("traceforge", d, allow_dirty=True)
        self.assertEqual(t.identity, "c" * 40 + "-dirty-" + "d" * 16)
        with self.assertRaisesRegex(build.BuildError, "allow-dirty-tools"):
            build.tool_identity("traceforge", d)

    @unittest.skipUnless(shutil.which("git"), "git")
    def test_ignored_project_file_is_a_snapshot(self):
        """Astra 7 / Opus F3."""
        g = self.s.path / "g"
        (g / "proj" / "rust" / "src").mkdir(parents=True)
        (g / "proj" / "daavar.toml").write_text("x")
        (g / "proj" / "rust" / "src" / "main.rs").write_text("fn main(){}")
        (g / "proj" / "rust" / "Cargo.toml").write_text("[package]")
        (g / ".gitignore").write_text("Cargo.lock\n")
        (g / "proj" / "rust" / "Cargo.lock").write_text("lock")
        env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        for a in (["init", "-q"], ["add", "-A"], ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x"]):
            subprocess.run(["git", "-C", str(g), *a], check=True, env=env, stdin=subprocess.DEVNULL)
        inputs = self.s.path / "inputs"
        inputs.mkdir()
        rec = provenance.project_inputs(g / "proj", inputs)
        self.assertEqual(rec["case"], "snapshot")
        self.assertEqual(rec["not_in_git"], ["rust/Cargo.lock"])
        self.assertEqual((inputs / "project" / "rust" / "Cargo.lock").read_text(), "lock")


class Reports(unittest.TestCase):
    def test_exact_nearest(self):
        """Astra 11: checked aaa, candidates a (distance 2) and abaa (distance 1)."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        ev = lambda c: {"s": {"t": c, "f": []}}
        mk = lambda w: {"statuses": {"c": "done"}, "words": {"c": [ev(x) for x in w]}, "counts": []}
        ref = s.path / "ref.jsonl"
        ref.write_text("{}\n" + json.dumps(mk("a")) + "\n" + json.dumps(mk("abaa")) + "\n")
        self.assertEqual([e["s"]["t"] for e in report.nearest(mk("aaa"), ref)["words"]["c"]], list("abaa"))

    def test_nearest_bound(self):
        """Opus F5: the direction's time bound, said."""
        ref = FIXTURES / "cases" / "tf-export-spec-ok" / "files" / "export.jsonl"
        line = json.loads((FIXTURES / "cases" / "tf-export-impl-bug" / "files" / "export.jsonl").read_text().splitlines()[2])
        n = report.Nearest(ref)
        with mock.patch.object(report, "NEAREST_TIME_S", -1.0):
            near, why = n.find(line)
        self.assertIn("gave up", why)

    def test_classifier_shape_and_cwd(self):
        """Astra 9, 10: a wrong-shaped answer is unclassified; the command runs in the run, its script absolute."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        (s.path / "proj").mkdir()
        (s.path / "proj" / "cl.py").write_text("import sys, json, os\nopen('here.txt','w').write(os.getcwd())\n"
                                               "for l in sys.stdin: print('null')\n")

        class Cfg:
            root = s.path / "proj"
            classify = {"command": f"{sys.executable} -I cl.py"}
        self.assertEqual(report.classifier_files(Cfg), ["cl.py"])
        self.assertEqual(report.classifier_argv(Cfg)[-1], str(s.path / "proj" / "cl.py"))
        jd = s.path / "run" / "c" / "joins" / "x"
        jd.mkdir(parents=True)
        e = type("E", (), {"cls": "no-slot", "line": "{}", "decide": None})()

        class DR:
            d = config.Direction("x", "s", "impl<=spec", True)
            join = Outcome("ok", "", data={"report": type("R", (), {"entries": [e]})()})

        DR.jd = jd

        class Chk:
            cfg = Cfg
            commands = []

            @staticmethod
            def log(m):
                pass
        res = report.classify(Chk, DR, s.path / "run")
        self.assertEqual(res, [("-", 'classifier output is not {"class", "note"}')])
        self.assertTrue((jd / "classify-work" / "here.txt").is_file())
        self.assertFalse((s.path / "proj" / "here.txt").exists())


class FromRun(unittest.TestCase):
    def test_selection_refused(self):
        """Opus F13."""
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            code = M.main(["check", "--from-run", "/nonexistent", "--cell", "a"])
        self.assertEqual((code, out.getvalue().strip()), (2, "daavar check: exit 2 REFUSED"))


if __name__ == "__main__":
    unittest.main()
