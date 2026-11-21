"""S7, S9, S10 units: run allocation and the lock, the word diff, the reproduction comparison, resolved TOML."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import json
import multiprocessing
import os
import shutil
import tomllib
import unittest

from daavar import config, provenance, report, rundir
from daavar.tests.util import FIXTURES, Scratch

CASES = FIXTURES / "cases"


def _alloc(out):
    return str(rundir.allocate(pathlib.Path(out), "s"))


class Runs(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()

    def tearDown(self):
        self.s.cleanup()

    def test_concurrent_allocation(self):
        out = self.s.path / "out"
        with multiprocessing.get_context("fork").Pool(8) as p:
            dirs = p.map(_alloc, [str(out)] * 64)
        self.assertEqual(len(set(dirs)), 64)
        self.assertTrue(all(pathlib.Path(d).is_dir() for d in dirs))

    def test_lock_no_wait(self):
        out = self.s.path / "out"
        with rundir.lock(out):
            r, w = os.pipe()
            pid = os.fork()
            if pid == 0:
                try:
                    with rundir.lock(out, wait=False):
                        os.write(w, b"got")
                except rundir.Busy:
                    os.write(w, b"busy")
                os._exit(0)
            os.waitpid(pid, 0)
            self.assertEqual(os.read(r, 10), b"busy")
        with rundir.lock(out, wait=False):
            pass

    def test_latest(self):
        out = self.s.path / "out"
        a = rundir.allocate(out, "s")
        rundir.set_latest(out, a)
        b = rundir.allocate(out, "s")
        rundir.set_latest(out, b)
        self.assertEqual(os.readlink(out / "latest"), b.name)


class Words(unittest.TestCase):
    def test_nearest_and_diff(self):
        line = json.loads((CASES / "tf-export-impl-bug" / "files" / "export.jsonl").read_text().splitlines()[2])
        ref = CASES / "tf-export-spec-ok" / "files" / "export.jsonl"
        near = report.nearest(line, ref)
        self.assertIsNotNone(near)
        wd = report.word_diff(line, near)
        self.assertEqual(sorted(wd), ["client", "server"])
        self.assertIn("- r Pong{1}", wd["client"])
        self.assertIn("+ r Pong{2}", wd["client"])
        self.assertEqual(report.word_text(line["words"]["client"]), "s Ping{1}, r Pong{2}, s Shutdown")
        self.assertEqual(report.event_text({"r": None}), "r ⊥")


class Reproduction(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        R = self.s.path / "R"
        for name, src in (("spec.jsonl", "tf-export-spec-ok"), ("rust.online-spec.jsonl", "tf-online-bug")):
            (R / "c.x" / "exports").mkdir(parents=True, exist_ok=True)
            shutil.copyfile(CASES / src / "files" / "export.jsonl", R / "c.x" / "exports" / name)
        j = R / "c.x" / "joins" / "rust-in-spec"
        shutil.copytree(CASES / "join-tf-bug" / "files" / "j", j)
        (R / "summary.tsv").write_text("\t".join(report.SUMMARY_COLUMNS) + "\n" + "\t".join(
            ["x"] * (len(report.SUMMARY_COLUMNS) - 3) + ["1", "2", "3"]) + "\n")
        self.R = R

    def tearDown(self):
        self.s.cleanup()

    def test_equal_up_to_timing(self):
        N = self.s.path / "N"
        shutil.copytree(self.R, N)
        # online timing, measurements and time columns differ: still equal
        p = N / "c.x" / "exports" / "rust.online-spec.jsonl"
        t = p.read_text().splitlines()
        h = json.loads(t[0])
        h["first_miss_wall_ms"] = 999
        p.write_text("\n".join([json.dumps(h, separators=(",", ":"))] + t[1:]) + "\n")
        jo = N / "c.x" / "joins" / "rust-in-spec" / "join.out"
        jo.write_text(jo.read_text().replace("wall_ms: ", "wall_ms: 7"))
        s = (N / "summary.tsv").read_text().replace("\t1\t2\t3\n", "\t9\t9\t9\n")
        (N / "summary.tsv").write_text(s)
        c = provenance.compare(self.R, N)
        self.assertTrue(c["equal"], [x for x in c["compared"] if not x["equal"]])
        # a changed export line is not
        p2 = N / "c.x" / "exports" / "spec.jsonl"
        p2.write_text(p2.read_text().replace('"Pong","f":[1]', '"Pong","f":[3]'))
        c = provenance.compare(self.R, N)
        self.assertFalse(c["equal"])
        self.assertEqual([x["file"] for x in c["compared"] if not x["equal"]], ["c.x/exports/spec.jsonl"])


class ResolvedToml(unittest.TestCase):
    def test_round_trip(self):
        for study in ("a2a-task", "mcp-cancel"):
            cfg = config.load(FIXTURES / "config" / f"{study}.daavar.toml")

            class C:  # the parts of a Check the writer reads
                model = cfg.model
                mode = "offline"
                stop = False

                class build:
                    tools = {"jmc": {"cli": "positional"}}
            text = provenance.resolved_toml(cfg, C)
            back = tomllib.loads(text)
            self.assertEqual(back["programs"], cfg.raw["programs"])
            self.assertEqual(back["scenarios"], cfg.raw["scenarios"])
            self.assertEqual(back["check"]["mode"], "offline")
            c2 = config.from_dict(back, cfg.root)
            self.assertEqual(len(config.select(c2)), len(config.select(cfg)))


if __name__ == "__main__":
    unittest.main()
