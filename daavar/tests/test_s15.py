"""S15 (§3.7, the JMC CLI switch): the named form's command line, its exits under §4.1, `cli = "auto"`, and the
pairing positional vs named, over fixtures captured on brain03 at JMC fc684f21 (fixtures/cases15/, kv15/;
capture/capture15.py, capture/test15.sh; DRIVER-IMPL-NOTES.md S15)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import hashlib
import json
import os
import unittest
import zipfile
from unittest import mock

from daavar import DaavarError, build, config, jmc, outcome
from daavar.tests.util import FIXTURES, Scratch

C15 = FIXTURES / "cases15"
KV = FIXTURES / "kv15"
FORMS = {"pos": "positional", "named": "named"}

# name: (kind, category, reason, reference side, checked side, enumeration, online verdict, stopped early);
# the same for both forms. Named exits differ (bounded 3, online reports 1, shape errors 2), the outcome not.
TABLE = {
    "export-impl-ok": ("ok", None, "", None, None, "complete", None, False),
    "export-spec-ok": ("ok", None, "", None, None, "complete", None, False),
    "export-impl-bug": ("ok", None, "", None, None, "complete", None, False),
    "export-impl-bug-bounded": ("ok", None, "", None, None, "bounded", None, False),
    "export-assert-spawned": ("failure", "failures", "jmc-failed", "refused", "violation", None, None, None),
    "export-assert-main": ("failure", "failures", "jmc-failed", "refused", "violation", None, None, None),
    "export-mainsend": ("refused", "harness", "shape", "refused", "refused", None, None, None),
    "export-novisible": ("refused", "roles", "shape", "refused", "refused", None, None, None),
    "export-float": ("refused", "values", "shape", "refused", "refused", None, None, None),
    "export-badargs": ("refused", "args", "usage", "refused", "refused", None, None, None),
    "export-badclass": ("refused", "args", "construct", "refused", "refused", None, None, None),
    "export-badstrategy": ("refused", "contributing", "tool-usage", "refused", "refused", None, None, None),
    "export-sleep": ("timeout", "budgets", "timeout", "incomplete", "incomplete", None, None, None),
    "export-sleep-jmctimeout": ("timeout", "budgets", "timeout", "incomplete", "incomplete", None, None, None),
    "export-unwritable": ("refused", "contributing", "tool-usage", "refused", "refused", None, None, None),
    "online-ok": ("ok", None, "", None, None, "complete", "conforms", False),
    "online-bug": ("ok", None, "", None, None, "complete", "reports", False),
    "online-bug-stop": ("ok", None, "", None, None, "bounded", "reports", True),
    "online-assert-spawned": ("failure", "failures", "jmc-failed", "refused", "violation", None, None, None),
    "online-spec-bounded": ("refused", "budgets", "tool-refused", "refused", "refused", None, None, None),
    "named-badopt": ("refused", "contributing", "tool-usage", "refused", "refused", None, None, None),
    "named-noout": ("refused", "contributing", "tool-usage", "refused", "refused", None, None, None),
}
NAMED_EXITS = {"export-impl-bug-bounded": 3, "online-bug": 1, "online-bug-stop": 1, "export-mainsend": 2,
               "export-novisible": 2, "export-float": 2}


def captured(form, name):
    d = C15 / form / name
    r = json.loads((d / "result.json").read_text())
    return r, (d / "stdout").read_text(), (d / "stderr").read_text(), d


def classify(form, name):
    r, out, err, d = captured(form, name)
    export = d / "files" / ("blocker/export.jsonl" if name == "export-unwritable" else "export.jsonl")
    return r, jmc.export_outcome(r["exit"], r["timed_out"], out, err, export, online=name.startswith("online"),
                                 graphs=d / "files" / "export.jsonl.graphs.jsonl", cli=FORMS[form])


def row(o):
    ex, ol = o.data.get("export"), o.data.get("online")
    return (o.kind, o.category, o.reason, outcome.side_verdict(o, "reference"), outcome.side_verdict(o, "checked"),
            ex.enumeration if ex else None, ol["verdict"] if ol else None, o.data.get("stopped_early"))


class NamedOutcomes(unittest.TestCase):
    def test_table_both_forms(self):
        for name, want in TABLE.items():
            for form in FORMS:
                if not (C15 / form / name).is_dir():
                    self.assertTrue(name.startswith("named-"), name)
                    continue
                r, o = classify(form, name)
                with self.subTest(form=form, name=name):
                    self.assertEqual(row(o), want, o.message)
                    self.assertEqual(o.exit, r["exit"])

    def test_named_exits_are_the_contract(self):
        """The named form's own statuses (NamedCli EXIT_HELP): 3 bounded, 1 an online report, 2 a shape error;
        the positional form exits 0, 0 and 1 for the same runs."""
        for name, code in NAMED_EXITS.items():
            self.assertEqual(captured("named", name)[0]["exit"], code, name)
            self.assertEqual(captured("pos", name)[0]["exit"], 0 if code in (1, 3) else 1, name)

    def test_named_mapping(self):
        self.assertEqual(jmc.named_to_positional(3, "exported:   1 iterations ...", False), 0)
        self.assertEqual(jmc.named_to_positional(1, "FAILED:     x", False), 1)
        self.assertEqual(jmc.named_to_positional(1, "", False), 1)  # an export exits 1 only with FAILED:
        self.assertEqual(jmc.named_to_positional(1, "verdict:    reports\n", True), 0)
        self.assertEqual(jmc.named_to_positional(2, "", False), 2)
        # 3 with FAILED: (an unfinished run): incomplete; 2 whose text reads as a failure: still refused
        o = jmc.export_outcome(3, False, "FAILED:     incomplete: a worker was lost\n", "", "/nonexistent",
                               cli="named")
        self.assertEqual((o.kind, o.reason, outcome.side_verdict(o, "checked")),
                         ("incomplete", "named-incomplete", "incomplete"))
        o = jmc.export_outcome(2, False, "FAILED:     java.lang.RuntimeException: boom\n", "", "/nonexistent",
                               cli="named")
        self.assertEqual((o.kind, o.reason, o.exit), ("refused", "named-refused", 2))
        # an exit the named form does not have
        o = jmc.export_outcome(4, False, "", "x", "/nonexistent", cli="named")
        self.assertEqual((o.kind, o.reason), ("refused", "other-exit"))
        with self.assertRaises(DaavarError):
            jmc.export_outcome(0, False, "", "", "/nonexistent", cli="auto")


class NamedMalformed(unittest.TestCase):
    def test_truncated_named_online_report_is_refused(self):
        """Astra S15 P2: a named online run (exit 1, no FAILED:) whose report is cut inside the uncovered line
        is refused as unparsable, never an exception out of the check."""
        r, out, err, d = captured("named", "online-bug")
        lines = out.splitlines()
        k = next(i for i, l in enumerate(lines) if l.startswith("uncovered:"))
        cut = "\n".join(lines[:k] + [lines[k][:len("uncovered:   2 gra")]]) + "\n"
        o = jmc.export_outcome(r["exit"], False, cut, err, d / "files" / "export.jsonl", online=True,
                               graphs=d / "files" / "export.jsonl.graphs.jsonl", cli="named")
        self.assertEqual((o.kind, o.reason, o.exit, outcome.side_verdict(o, "checked")),
                         ("refused", "unparsable", 1, "refused"))


class NamedArgv(unittest.TestCase):
    """jmc.named() and jmc.positional() build exactly the command lines the S15 fixtures ran."""

    def argv(self, f, name, **k):
        cmd = json.loads((C15 / ("pos" if f is jmc.positional else "named") / name / "cmd.json").read_text())["argv"]
        cp = cmd[cmd.index("-cp") + 1].split(":")
        return cmd, f(cmd[0], "2g", cp, "org.mpi_sws.jmc.mp.launch.MpLauncher", k.pop("prog"), 1, "export.jsonl",
                      "export.jsonl.graphs.jsonl", 600, **k)

    def test_export_and_online(self):
        for f in (jmc.positional, jmc.named):
            cmd, a = self.argv(f, "export-impl-ok", prog="fx.Fx:impl,ok")
            self.assertEqual(a, cmd)
            spec = json.loads((C15 / "pos" / "online-bug-stop" / "cmd.json").read_text())["argv"]
            spec = spec[spec.index("--spec") + 1]
            cmd, a = self.argv(f, "online-bug-stop", prog="fx.Fx:impl,bug", spec=spec, stop_at_first=True)
            self.assertEqual(a, cmd)
        with self.assertRaises(DaavarError):
            jmc.named("java", "2g", [], "M", "P", 1, "e", "g", 60, stop_at_first=True)
        self.assertEqual(jmc.argv("named", "java", "2g", ["c"], "M", "P", 1, "e", "g", 60)[6:8], ["M", "export"])


class Auto(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.d = self.s.path

    def tearDown(self):
        self.s.cleanup()

    def test_has_named_cli(self):
        classes = self.d / "classes"
        (classes / "org/mpi_sws/jmc/mp/launch").mkdir(parents=True)
        (classes / "org/mpi_sws/jmc/mp/launch/MpLauncher.class").write_bytes(b"")
        self.assertFalse(jmc.has_named_cli([classes, self.d / "missing.jar"]))
        jar = self.d / "core.jar"
        with zipfile.ZipFile(jar, "w") as z:
            z.writestr(jmc.NAMED_CLI_CLASS, b"")
        (self.d / "bad.jar").write_bytes(b"not a zip")
        self.assertTrue(jmc.has_named_cli([classes, self.d / "bad.jar", jar]))
        (classes / "org/mpi_sws/jmc/mp/cli").mkdir()
        (classes / jmc.NAMED_CLI_CLASS).write_bytes(b"")
        self.assertTrue(jmc.has_named_cli([classes]))

    def test_resolve_once_per_build(self):
        with mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(self.d / "cache")}):
            classes = self.d / "c"
            (classes / "org/mpi_sws/jmc/mp/cli").mkdir(parents=True)
            (classes / jmc.NAMED_CLI_CLASS).write_bytes(b"")
            self.assertEqual(build.resolve_cli("positional", {"identity": "x", "classpath": [classes]}), "positional")
            self.assertFalse(build.cli_cache_path("x").exists())
            self.assertEqual(build.resolve_cli("auto", {"identity": "x", "classpath": [classes]}), "named")
            self.assertEqual(json.loads(build.cli_cache_path("x").read_text())["cli"], "named")
            # cached: the classpath is not looked at again
            self.assertEqual(build.resolve_cli("auto", {"identity": "x", "classpath": []}), "named")
            self.assertEqual(build.resolve_cli("auto", {"identity": "old", "classpath": [self.d]}), "positional")
            self.assertEqual(json.loads(build.cli_cache_path("old").read_text())["cli"], "positional")

    def test_config_default_positional(self):
        import tomllib
        conf = FIXTURES / "config"
        with open(conf / "a2a-task.daavar.toml", "rb") as f:
            raw = tomllib.load(f)
        raw["tools"]["jmc"].pop("cli", None)
        self.assertEqual(config.from_dict(raw, conf).tools["jmc"]["cli"], "positional")
        raw["tools"]["jmc"]["cli"] = "auto"
        self.assertEqual(config.from_dict(raw, conf).tools["jmc"]["cli"], "auto")

def norm_verdict(v):
    """A verdict.json without what names the run (its id, export keys: the key holds the CLI form)."""
    out = {k: v[k] for k in ("status", "exit", "gating", "all", "model", "mode", "vocab")}
    out["cells"] = [{"cell": c["cell"], "status": c["status"], "directions": [
        {k: d.get(k) for k in ("direction", "form", "gating", "status", "label", "aggregates_as", "note", "reason",
                                "uncovered")} | {"enumerations": ((d.get("checked") or {}).get("enumeration"),
                                                                  (d.get("ref") or {}).get("enumeration"))}
        for d in c["directions"]]} for c in v["cells"]]
    return out


def md5(p):
    return hashlib.md5(p.read_bytes()).hexdigest()


class KvCancelPaired(unittest.TestCase):
    """kv-cancel checked on brain03 with JMC fc684f21 under cli positional, named and auto (test15.sh)."""

    def test_exports_byte_identical_and_verdicts_identical(self):
        for mode in ("offline", "online"):
            p, n = KV / f"positional-{mode}", KV / f"named-{mode}"
            files = sorted(x.relative_to(p) for x in p.rglob("*.jsonl*") if "exports" in x.parts)
            self.assertTrue(any(f.name.startswith("java") for f in files), mode)
            self.assertEqual(files, sorted(x.relative_to(n) for x in n.rglob("*.jsonl*") if "exports" in x.parts))
            for f in files:
                self.assertEqual(md5(p / f), md5(n / f), f"{mode} {f}")
            vp, vn = (json.loads((x / "verdict.json").read_text()) for x in (p, n))
            self.assertEqual(norm_verdict(vp), norm_verdict(vn), mode)
            self.assertEqual(vp["status"], "CONFORMS")
            self.assertEqual((p / "summary.tsv").read_text().split("\t")[:3],
                             (n / "summary.tsv").read_text().split("\t")[:3])

    def test_command_lines_and_reclassification(self):
        for mode in ("offline", "online"):
            for form in ("positional", "named"):
                ex = KV / f"{form}-{mode}" / "one.impl" / "exports"
                stems = ["java"] + (["java.online-spec"] if mode == "online" else [])
                prov = json.loads((KV / f"{form}-{mode}" / "provenance.json").read_text())
                recs = [c for c in prov["commands"] if c.get("program") == "java"]
                self.assertEqual(len(recs), len(stems))
                for stem in stems:
                    cmd = (ex / f"{stem}.cmd").read_text().split()
                    main = cmd.index("org.mpi_sws.jmc.mp.launch.MpLauncher")
                    online = stem != "java"
                    if form == "named":
                        self.assertEqual(cmd[main + 1], "online" if online else "export")
                    else:
                        self.assertEqual(cmd[main + 1], "bench.kv_cancel.KvCancel:impl")
                    rec = next(c for c in recs if c["step"] == ("online" if online else "offline") + "-export")
                    o = jmc.export_outcome(rec["exit"], False, (ex / f"{stem}.out").read_text(),
                                           (ex / f"{stem}.err").read_text(), ex / f"{stem}.jsonl", online=online,
                                           graphs=ex / f"{stem}.jsonl.graphs.jsonl", cli=form)
                    self.assertTrue(o.ok, (form, mode, stem, o.message))
                    self.assertEqual(o.data["export"].enumeration, "complete")

    def test_auto(self):
        for mode in ("offline", "online"):
            a, n = KV / f"auto-{mode}", KV / f"named-{mode}"
            self.assertEqual(norm_verdict(json.loads((a / "verdict.json").read_text())),
                             norm_verdict(json.loads((n / "verdict.json").read_text())))
            self.assertIn(" export --program ", (a / "one.impl" / "exports" / "java.cmd").read_text())
        # JMC c1bbf657 (no mp.cli package): auto is positional, and the export's summaries are the same bytes
        old = KV / "old-auto-offline" / "one.impl" / "exports"
        self.assertIn(" bench.kv_cancel.KvCancel:impl must 0 1 ", (old / "java.cmd").read_text())
        self.assertEqual(md5(old / "java.jsonl"), md5(KV / "named-offline" / "one.impl" / "exports" / "java.jsonl"))
        self.assertEqual(json.loads((KV / "old-auto-offline" / "verdict.json").read_text())["status"], "CONFORMS")


if __name__ == "__main__":
    unittest.main()
