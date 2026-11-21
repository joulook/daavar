"""S8: the §4.2-§4.3 truth tables over synthetic outcomes (the parsers are tested over the S3 fixtures)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import unittest

from daavar import config, outcome, verdict
from daavar.outcome import Outcome
from daavar.parse import ExportFile, JoinEntry, JoinReport


def exp(enum="complete", stopped=False, lines=3):
    return Outcome("ok", "", data={"export": ExportFile("x", {"enumeration": enum}, lines), "stopped_early": stopped})


def fail(kind, msg="m", cat="failures"):
    return Outcome(kind, msg, cat, kind)


def join(unc=0, viol=0, fa=0, unres=0):
    rep = JoinReport({}, {}, "violation" if unc else "conforms", "", unc, None,
                     {"uncovered": unc, "violations": viol, "false_alarms": fa, "unresolved": unres},
                     [JoinEntry("not-found", "{}")] * unc)
    return Outcome("ok", "", data={"report": rep})


D = verdict.direction


class Table42(unittest.TestCase):
    def test_rows(self):
        self.assertEqual(D(exp("bounded"), exp(), None).status, "incomplete")      # reference not complete
        self.assertEqual(D(exp(), exp(), join(2, viol=1, fa=1)).status, "violation")
        v = D(exp(), exp("aborted", stopped=True), join(1, fa=1))
        self.assertEqual((v.status, v.note), ("incomplete", verdict.STOP_FA))
        v = D(exp(), exp("bounded", stopped=True), join(1, unres=1))
        self.assertEqual((v.status, v.note), ("incomplete", verdict.STOP_UNRES))
        self.assertEqual(D(exp(), exp("aborted", stopped=True), join(1, viol=1)).status, "violation")
        self.assertEqual(D(exp(), exp(), join(0)).status, "conforms")
        v = D(exp(), exp(), join(3, fa=3))
        self.assertEqual((v.status, v.note), ("conforms", "3 false alarms"))
        self.assertEqual(D(exp(), exp(), join(2, fa=1, unres=1)).status, "incomplete")
        self.assertEqual(D(exp(), exp("bounded"), join(0)).status, "incomplete")
        self.assertEqual(D(exp(), exp("sampled"), join(1, fa=1)).status, "incomplete")

    def test_41_replaces_the_row(self):
        self.assertEqual(D(fail("failure"), exp(), None).status, "refused")         # reference program failure
        self.assertEqual(D(exp(), fail("failure"), None).status, "violation")       # checked program failure
        self.assertEqual(D(fail("timeout"), exp(), None).status, "incomplete")
        self.assertEqual(D(exp(), fail("timeout"), None).status, "incomplete")
        self.assertEqual(D(exp(), fail("refused", "usage: x", "args"), None).status, "refused")  # a usage exit
        self.assertEqual(D(fail("refused"), fail("failure"), None).status, "refused")  # the reference first
        self.assertEqual(D(exp(), exp(), fail("refused", "join", "model")).status, "refused")
        self.assertEqual(D(exp(), exp(), None).status, "refused")


def dirn(form, gating=True, name="x<=s"):
    c, r = name.split("<=")
    return config.Direction(c, r, form, gating)


class Table43(unittest.TestCase):
    def agg(self, cells):
        return verdict.aggregate([(n, [(dirn(*d[:2], name=f"p{i}<=s"), verdict.DirVerdict(d[2]))
                                       for i, d in enumerate(ds)]) for n, ds in cells])

    def test_precedence_and_exit(self):
        a = self.agg([("c1", [("impl<=spec", True, "conforms"), ("impl<=spec", True, "incomplete")]),
                      ("c2", [("impl<=spec", True, "violation")])])
        self.assertEqual((a.status, a.exit, a.cells), ("VIOLATION", 1, {"c1": "incomplete", "c2": "violation"}))
        # A violation anywhere decides the run (D23): within a cell and across cells.
        a = self.agg([("c", [("impl<=spec", True, "violation"), ("impl<=spec", True, "refused")])])
        self.assertEqual((a.status, a.exit, a.cells["c"]), ("VIOLATION", 1, "violation"))
        a = self.agg([("c1", [("impl<=spec", True, "violation")]), ("c2", [("impl<=spec", True, "refused")])])
        self.assertEqual((a.status, a.exit), ("VIOLATION", 1))
        a = self.agg([("c1", [("impl<=spec", True, "incomplete")]), ("c2", [("impl<=spec", True, "refused")])])
        self.assertEqual((a.status, a.exit), ("REFUSED", 2))
        a = self.agg([("c", [("mutant<=reference", True, "violation"), ("impl<=spec", True, "refused")])])
        self.assertEqual((a.status, a.exit), ("REFUSED", 2))  # a rejected mutant is conforms, not a violation
        self.assertEqual(self.agg([("c", [("impl<=spec", True, "incomplete")])]).exit, 2)
        self.assertEqual(self.agg([("c", [("impl<=spec", True, "conforms")])]).exit, 0)

    def test_mutants(self):
        a = self.agg([("c", [("mutant<=reference", True, "violation")])])
        self.assertEqual((a.status, a.exit), ("CONFORMS", 0))  # rejected
        a = self.agg([("c", [("mutant<=reference", True, "conforms")])])
        self.assertEqual((a.status, a.exit), ("INCOMPLETE", 2))  # not rejected
        v = verdict.DirVerdict("violation")
        self.assertEqual((v.label("mutant<=reference"), v.label("impl<=spec")), ("rejected", "violation"))

    def test_report_only(self):
        a = self.agg([("c", [("impl<=impl", False, "violation")])])
        self.assertEqual((a.status, a.exit, a.gating, a.all), ("REPORT ONLY", 2, None, "violation"))

    def test_twins_do_not_gate(self):
        a = self.agg([("c", [("impl<=spec", True, "conforms"), ("impl<=impl", False, "violation")])])
        self.assertEqual((a.status, a.exit, a.all), ("CONFORMS", 0, "violation"))


if __name__ == "__main__":
    unittest.main()
