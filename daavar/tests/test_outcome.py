"""S4: the §4.1 classification and the parsers over the S3 fixtures (captured on brain03, see
fixtures/capture/ and DRIVER-IMPL-NOTES.md S3), plus synthetic cases for rows no program can plant."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import json
import shutil
import unittest

from daavar import errors, jmc, outcome, parse, tf
from daavar.tests.util import FIXTURES, Scratch

CASES = FIXTURES / "cases"


def case(name):
    d = CASES / name
    r = json.loads((d / "result.json").read_text())
    return r, (d / "stdout").read_text(), (d / "stderr").read_text(), d / "files"


def cls(tool, step, name):
    r, out, err, _ = case(name)
    return outcome.classify(tool, step, r["exit"], r["timed_out"], out, err)


# name: (tool, step, kind, category, reason, verdict on the reference side, verdict on the checked side)
TABLE = {
    "tf-export-spec-ok": ("tf", "export", "ok", None, "", None, None),
    "tf-export-impl-bug": ("tf", "export", "ok", None, "", None, None),
    "tf-export-impl-panic": ("tf", "export", "failure", "failures", "panic", "refused", "violation"),
    "tf-export-spec-panic": ("tf", "export", "failure", "failures", "panic", "refused", "violation"),
    "tf-export-impl-stdassert": ("tf", "export", "failure", "failures", "panic", "refused", "violation"),
    "tf-export-impl-assert": ("tf", "export", "failure", "failures", "assertion", "refused", "violation"),
    "tf-export-spec-assert": ("tf", "export", "failure", "failures", "assertion", "refused", "violation"),
    "tf-export-usage": ("tf", "export", "refused", "args", "usage", "refused", "refused"),
    "a2a-tf-export-usage-panic": ("tf", "export", "refused", "args", "usage", "refused", "refused"),
    "tf-export-badvis": ("tf", "export", "refused", "roles", "tool-refused", "refused", "refused"),
    "tf-export-badopt": ("tf", "export", "refused", "contributing", "tool-usage", "refused", "refused"),
    "tf-export-sleep": ("tf", "export", "timeout", "budgets", "timeout", "incomplete", "incomplete"),
    "tf-online-ok": ("tf", "online", "ok", None, "", None, None),
    "tf-online-panic": ("tf", "online", "failure", "failures", "online-failed", "refused", "violation"),
    "tf-online-assert": ("tf", "online", "failure", "failures", "online-failed", "refused", "violation"),
    "tf-online-stop-threads2": ("tf", "online", "refused", "contributing", "tool-usage", "refused", "refused"),
    "tf-online-spec-bounded": ("tf", "online", "refused", "budgets", "tool-refused", "refused", "refused"),
    "jmc-export-impl-ok": ("jmc", "export", "ok", None, "", None, None),
    "jmc-export-assert-spawned": ("jmc", "export", "failure", "failures", "jmc-failed", "refused", "violation"),
    "jmc-export-assert-main": ("jmc", "export", "failure", "failures", "jmc-failed", "refused", "violation"),
    "jmc-online-assert-spawned": ("jmc", "online", "failure", "failures", "jmc-failed", "refused", "violation"),
    "jmc-export-mainsend": ("jmc", "export", "refused", "harness", "shape", "refused", "refused"),
    "jmc-export-novisible": ("jmc", "export", "refused", "roles", "shape", "refused", "refused"),
    "jmc-export-float": ("jmc", "export", "refused", "values", "shape", "refused", "refused"),
    "jmc-export-badargs": ("jmc", "export", "refused", "args", "usage", "refused", "refused"),
    "jmc-export-badclass": ("jmc", "export", "refused", "args", "construct", "refused", "refused"),
    "jmc-export-badstrategy": ("jmc", "export", "refused", "contributing", "tool-usage", "refused", "refused"),
    "jmc-export-sleep": ("jmc", "export", "timeout", "budgets", "timeout", "incomplete", "incomplete"),
    "jmc-online-spec-bounded": ("jmc", "online", "refused", "budgets", "tool-refused", "refused", "refused"),
    "join-tf-ok": ("conf-join", "join", "ok", None, "", None, None),
    "join-tf-bug": ("conf-join", "join", "ok", None, "", None, None),
    "join-refused-model": ("conf-join", "join", "refused", "model", "join-refused", "refused", "refused"),
    "join-refused-incomplete": ("conf-join", "join", "refused", "budgets", "join-refused", "refused", "refused"),
    "join-refused-bounded-jmc": ("conf-join", "join", "refused", "budgets", "join-refused", "refused", "refused"),
    "conf-join-usage": ("conf-join", "join", "refused", "contributing", "tool-usage", "refused", "refused"),
}


class Classification(unittest.TestCase):
    def test_table(self):
        for name, (tool, step, kind, cat, reason, ref, chk) in TABLE.items():
            with self.subTest(case=name):
                o = cls(tool, step, name)
                self.assertEqual((o.kind, o.category, o.reason), (kind, cat, reason), o.message)
                self.assertEqual(outcome.side_verdict(o, "reference"), ref)
                self.assertEqual(outcome.side_verdict(o, "checked"), chk)
                if kind != "ok":
                    self.assertTrue(o.message)
                    errors.render("x", o.kind, o.message, o.category, "log")

    def test_messages_are_the_tools_text(self):
        self.assertIn("fx: planted panic", cls("tf", "export", "tf-export-impl-panic").message)
        self.assertEqual(cls("tf", "online", "tf-online-panic").message, "fx: planted panic")
        self.assertIn("usage: tf_fx", cls("tf", "export", "tf-export-usage").message)
        self.assertIn("unknown variant `q`", cls("tf", "export", "a2a-tf-export-usage-panic").message)
        self.assertIn("main performs a send", cls("jmc", "export", "jmc-export-mainsend").message)
        self.assertIn("planted assert in the server", cls("jmc", "export", "jmc-export-assert-spawned").message)
        self.assertIn("the two models differ: p2p and asyn", cls("conf-join", "join", "join-refused-model").message)

    def test_jvm_sigterm_exit_143_is_a_timeout_only_when_the_driver_sent_it(self):
        r, out, err, _ = case("jmc-export-sleep")
        self.assertEqual(r["exit"], 143)
        self.assertEqual(outcome.classify("jmc", "export", 143, True, out, err).kind, "timeout")
        o = outcome.classify("jmc", "export", 143, False, out, err)
        self.assertEqual((o.kind, o.reason), ("refused", "other-exit"))

    def test_synthetic_rows(self):
        # TraceForge "configuration is refused" (program.rs:673): no fixture plants it (DRIVER-IMPL-NOTES S3).
        o = outcome.classify("tf", "export", 2, False, "", "tf_fx: the configuration is refused: ConfigError { field: Mode }\n")
        self.assertEqual((o.kind, o.reason), ("refused", "configuration"))
        o = outcome.classify("jmc", "export", 1, False, "FAILED:     java.lang.OutOfMemoryError: Java heap space\n", "")
        self.assertEqual((o.kind, o.category), ("refused", "budgets"))  # the launcher's JVM, no process named
        o = outcome.classify("tf", "export", -11, False, "", "")
        self.assertEqual((o.kind, o.message), ("refused", "killed by signal 11"))
        o = outcome.classify("tf", "export", 1, False, "", "boom\n")
        self.assertEqual((o.kind, o.message), ("refused", "exit 1: boom"))
        o = outcome.classify("tf", "export", 1, False, "FAILED:     x\n", "")  # FAILED from export is not online
        self.assertEqual(o.kind, "refused")
        o = outcome.classify("jmc", "export", 1, False, "no failed line\n", "")
        self.assertEqual(o.kind, "refused")
        o = outcome.classify("conf-join", "join", 3, False, "", "")
        self.assertEqual(o.kind, "refused")
        with self.assertRaises(ValueError):
            outcome.side_verdict(o, "spec")

    def test_jmc_output_failure_is_refused(self):
        msg = "FAILED:     must: process m/0 (task 2) failed: java.lang.AssertionError: x; cannot write e.jsonl: java.nio.file.AccessDeniedException\n"
        r0, out0, err0, files0 = case("jmc-export-assert-spawned")
        o = jmc.export_outcome(1, False, msg, "", files0 / "missing.jsonl")
        self.assertEqual((o.kind, o.reason), ("refused", "output-failed"))
        self.assertEqual(outcome.side_verdict(o, "checked"), "refused")
        o = outcome.classify("jmc", "export", 1, False, "FAILED:     cannot write witness w/: x\n", "")
        self.assertEqual(o.reason, "output-failed")
        # The program's own exception text is the program's: a violation on the checked side.
        msg = "FAILED:     must: process m/0 (task 2) failed: java.lang.AssertionError: cannot write response: busy\n"
        o = outcome.classify("jmc", "export", 1, False, msg, "")
        self.assertEqual(outcome.side_verdict(o, "checked"), "violation")
        # `; cannot write` after a program failure: the export decides (written: the program's text).
        r, out, err, files = case("jmc-export-assert-spawned")
        out2 = out.replace("fx: planted assert in the server", "request failed; cannot write response")
        o = jmc.export_outcome(1, False, out2, err, files / "export.jsonl")
        self.assertEqual(outcome.side_verdict(o, "checked"), "violation")
        o = jmc.export_outcome(1, False, out2, err, files / "missing.jsonl")
        self.assertEqual((o.kind, o.reason), ("refused", "output-failed"))
        # The export written but the graph store not (MpLauncher.java:673): the launcher's failure.
        o = jmc.export_outcome(1, False, out2, err, files / "export.jsonl", graphs=files / "missing.graphs.jsonl")
        self.assertEqual((o.kind, o.reason), ("refused", "output-failed"))

    def test_record_named_like_an_assertion_is_still_a_shape_error(self):
        msg = "FAILED:     must: record fx.Fx$AssertionErrorMessage has no @Tag; the format needs a tag\n"
        o = outcome.classify("jmc", "export", 1, False, msg, "")
        self.assertEqual((o.kind, o.category), ("refused", "values"))

    def test_assertion_text_is_not_a_shape_error(self):
        msg = "FAILED:     must: process m/0 (task 2) failed: java.lang.AssertionError: slot is reserved\n"
        o = outcome.classify("jmc", "export", 1, False, msg, "")
        self.assertEqual((o.kind, outcome.side_verdict(o, "checked")), ("failure", "violation"))

    def test_oom_in_a_process_is_a_program_failure(self):
        """Review S1-S6 A3 (D4 rejected): a throwable raised in a process, OutOfMemoryError included."""
        o = outcome.classify("jmc", "export", 1, False, "FAILED: must: process m/0 (task 2) failed: "
                             "java.lang.OutOfMemoryError: planted\n", "")
        self.assertEqual((o.kind, outcome.side_verdict(o, "checked"), outcome.side_verdict(o, "reference")),
                         ("failure", "violation", "refused"))

    def test_main_thread_exception_is_not_a_shape_error(self):
        """Review S1-S6 A2: `throw new IllegalStateException("slot is reserved")` in main."""
        for msg in ("FAILED: slot is reserved\n", "FAILED:     java.lang.IllegalStateException: slot is reserved\n",
                    "FAILED:     main performs a send\n", "FAILED:     record X has no @Tag; whatever\n"):
            o = outcome.classify("jmc", "export", 1, False, msg, "")
            self.assertEqual(outcome.side_verdict(o, "checked"), "violation", msg)

    def test_tf_assertion_both_modes(self):
        """Ruling on review S1-S6 F3: a Rust assertion is a program failure offline as online."""
        for name in ("tf-export-impl-assert", "tf-online-assert"):
            o = cls("tf", "export" if "export" in name else "online", name)
            self.assertEqual((outcome.side_verdict(o, "checked"), outcome.side_verdict(o, "reference")),
                             ("violation", "refused"), name)

    def test_render(self):
        o = cls("jmc", "export", "jmc-export-mainsend")
        r, out, err, _ = case("jmc-export-mainsend")
        text = errors.render("java", o.kind, o.message, o.category, "exports/java.out", out)
        self.assertIn("java: refused: must: main performs a send", text)
        self.assertIn("See docs/modelling.md#harness. Fix:", text)
        self.assertIn("Log: exports/java.out", text)
        self.assertLessEqual(text.count("\n"), 2 + errors.EXCERPT_LINES + 2)


class Adapters(unittest.TestCase):
    def export(self, mod, name, **k):
        r, out, err, files = case(name)
        return mod.export_outcome(r["exit"], r["timed_out"], out, err, files / "export.jsonl", **k)

    def test_tf_exports(self):
        o = self.export(tf, "tf-export-spec-ok")
        self.assertTrue(o.ok)
        ex = o.data["export"]
        self.assertEqual((ex.lines, ex.enumeration, ex.header["tool"], ex.header["program"], ex.header["size"]),
                         (1, "complete", "traceforge", "fx", "spec,ok"))
        self.assertEqual(o.data["store"], {"written": 1, "refused": 0, "ms": 0})
        self.assertEqual(o.data["measurements"]["stop_reason"], "exhausted")
        self.assertEqual(self.export(tf, "tf-export-impl-bug").data["export"].lines, 2)
        self.assertEqual(self.export(tf, "tf-export-impl-bug-bag").data["export"].header["model"], "asyn")
        o = self.export(tf, "tf-export-threads2")
        self.assertEqual((o.data["export"].lines, o.data["store"]["written"]), (2, 0))
        self.assertEqual(self.export(tf, "tf-export-impl-panic").kind, "failure")
        self.assertEqual(self.export(tf, "a2a-tf-export-impl-b").data["export"].lines, 5)

    def test_tf_online(self):
        def onl(name):
            r, out, err, files = case(name)
            return tf.online_outcome(r["exit"], r["timed_out"], out, err, files / "export.jsonl")
        o = onl("tf-online-ok")
        self.assertEqual((o.data["online"]["verdict"], o.data["stopped_early"], o.data["export"].enumeration),
                         ("conforms", False, "complete"))
        o = onl("tf-online-bug")
        self.assertEqual((o.data["online"]["verdict"], o.data["online"]["uncovered_total"], o.data["stopped_early"]),
                         ("reports", 1, False))
        o = onl("tf-online-bug-stop")
        self.assertEqual((o.data["stopped_early"], o.data["export"].enumeration, o.data["online"]["first_miss_iteration"]),
                         (True, "aborted", 1))
        self.assertTrue(o.data["online"]["bug_finder"])
        o = onl("tf-online-bug-budget")
        self.assertEqual((o.data["export"].enumeration, o.data["stopped_early"]), ("bounded", False))
        self.assertEqual(onl("tf-online-panic").kind, "failure")

    def test_jmc_exports_and_online(self):
        o = self.export(jmc, "jmc-export-impl-ok")
        self.assertEqual((o.data["export"].lines, o.data["export"].header["tool"]), (1, "jmc"))
        o = self.export(jmc, "jmc-export-impl-bug-bounded")
        self.assertEqual(o.data["export"].enumeration, "bounded")
        self.assertEqual(o.data["measurements"]["stop_reason"], "budget")
        o = self.export(jmc, "jmc-online-bug-stop", online=True)
        self.assertEqual((o.data["stopped_early"], o.data["export"].enumeration, o.data["online"]["verdict"]),
                         (True, "bounded", "reports"))
        o = self.export(jmc, "jmc-online-ok", online=True)
        self.assertEqual((o.data["online"]["verdict"], o.data["stopped_early"]), ("conforms", False))
        o = self.export(jmc, "jmc-export-mainsend")
        self.assertEqual(o.kind, "refused")
        self.assertEqual(self.export(jmc, "a2a-jmc-export-impl-b").data["export"].lines, 5)

    def tampered(self, name, fn, mod=tf, kind="online", out_fn=None):
        s = Scratch()
        self.addCleanup(s.cleanup)
        r, out, err, files = case(name)
        out = out_fn(out) if out_fn else out
        p = s.path / "export.jsonl"
        lines = (files / "export.jsonl").read_text().splitlines(keepends=True)
        p.write_text("".join(fn(lines)))
        if mod is tf and kind == "online":
            return tf.online_outcome(r["exit"], r["timed_out"], out, err, p)
        if mod is tf:
            return tf.export_outcome(r["exit"], r["timed_out"], out, err, p)
        return jmc.export_outcome(r["exit"], r["timed_out"], out, err, p, online=(kind == "online"))

    def test_truncated_or_malformed_exports_refused(self):
        drop_last = lambda ls: ls[:-1]
        for name, mod, kind in (("tf-online-bug", tf, "online"), ("tf-export-impl-bug", tf, "export"),
                                ("jmc-export-impl-bug", jmc, "export"), ("jmc-online-bug", jmc, "online")):
            o = self.tampered(name, drop_last, mod, kind)
            self.assertEqual(o.reason, "unparsable", name)
            self.assertRegex(o.message, "summaries_distinct|`exported:` says")
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + ["not json\n"] + ls[2:])
        self.assertIn("not JSON", o.message)
        o = self.tampered("tf-online-bug", lambda ls: [ls[0].replace(',"roles":["client","server"]', "")] + ls[1:])
        self.assertIn("needs roles", o.message)
        o = self.tampered("tf-online-bug", lambda ls: [ls[0].replace('"complete"', '"bounded"')] + ls[1:])
        self.assertIn("stop_reason exhausted", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('"counts":[', '"counts":[9,')] + ls[2:])
        self.assertIn("counts", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('{"s":{"t":"Ping","f":[1]}}', '42', 1)] + ls[2:])
        self.assertIn("word event", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('"f":[1]', '"f":[1.5]', 1)] + ls[2:])
        self.assertIn("float", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('"counts":[0,2', '"counts":[0,7', 1)] + ls[2:])
        self.assertIn("exceeds", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('"f":[1]', '"f":[-0]', 1)] + ls[2:])
        self.assertIn("-0", o.message)
        for a, b in (('"minor":1', '"minor":2'), ('"minor":1', '"minor":1.0'), ('"format":1', '"format":true'),
                     ('"encoding":1', '"encoding":1.0')):
            o = self.tampered("tf-online-bug", lambda ls: [ls[0].replace(a, b)] + ls[1:])
            self.assertEqual(o.reason, "unparsable", b)
        o = self.tampered("tf-online-bug", lambda ls: ls[:1] + [ls[1].replace('"t":"Ping"', '"t":"\\ud800"', 1)] + ls[2:])
        self.assertIn("word event", o.message)
        o = self.tampered("tf-online-bug", lambda ls: [ls[0].replace('"visible":["client"', '"visible":["\\ud800"')] + ls[1:])
        self.assertEqual(o.reason, "unparsable")
        o = self.tampered("tf-online-bug", lambda ls: ls[:2] + ["\n"] + ls[2:])
        self.assertIn("blank line", o.message)
        o = self.tampered("tf-online-bug", lambda ls: ls[:2] + ls[1:2])
        self.assertTrue(o.reason == "unparsable")
        o = self.tampered("tf-online-bug", lambda ls: [ls[0].replace('"roles":["client","server"]', '"roles":["server","client"]')] + ls[1:])
        self.assertIn("roles", o.message)

    def test_jmc_first_miss_on_the_last_graph(self):
        """MpLauncher writes complete when the stack is exhausted at the stop (MpLauncher.java:622, 1095)."""
        o = self.tampered("jmc-online-bug-stop", lambda ls: [ls[0].replace('"bounded"', '"complete"')] + ls[1:],
                          jmc, "online", lambda t: t.replace("(bounded, 0 not exported)", "(complete, 0 not exported)"))
        self.assertTrue(o.ok, o.message)
        self.assertTrue(o.data["stopped_early"])

    def test_tool_mismatch_and_missing_export_refused(self):
        r, out, err, files = case("tf-export-spec-ok")
        o = jmc.export_outcome(0, False, out, err, files / "export.jsonl")
        self.assertEqual((o.kind, o.reason), ("refused", "unparsable"))
        o = tf.export_outcome(0, False, out, err, files / "nothing.jsonl")
        self.assertEqual((o.kind, o.reason), ("refused", "unparsable"))

class Joins(unittest.TestCase):
    def join(self, name, d=None):
        r, out, err, files = case(name)
        return tf.join_outcome(r["exit"], r["timed_out"], out, err, d or files / "j")

    def test_conforming_and_violating(self):
        o = self.join("join-tf-ok")
        rep = o.data["report"]
        self.assertEqual((rep.verdict, rep.uncovered, rep.decide_counts()),
                         ("conforms", 0, {"violations": 0, "false_alarms": 0, "unresolved": 0}))
        o = self.join("join-tf-bug")
        rep = o.data["report"]
        self.assertEqual((rep.verdict, rep.uncovered, rep.entries[0].cls, rep.entries[0].decide["outcome"]),
                         ("violation", 1, "no-slot", "violation"))
        self.assertEqual(rep.entries[0].graph, "1.graph.json")
        self.assertEqual(o.data["index"][0]["reason"], "ok")
        self.assertEqual(rep.spec["size"], "spec,ok")
        o = self.join("join-tf-online-stop")
        self.assertEqual((o.data["report"].label, o.data["report"].imp["enumeration"]), ("partial", "aborted"))
        for name, n in (("a2a-join-rust-b", 5), ("a2a-join-java-b", 5), ("join-jmc-bug", 1)):
            rep = self.join(name).data["report"]
            self.assertEqual((rep.uncovered, rep.decide["violations"], len(rep.entries)), (n, n, n), name)
        self.assertEqual(self.join("join-twin-jmc-in-tf").data["report"].verdict, "conforms")
        self.assertIsNotNone(self.join("join-merge-unsorted").data["report"].merge)

    def test_refusals(self):
        self.assertEqual(self.join("join-refused-model").category, "model")
        self.assertEqual(self.join("join-refused-incomplete").category, "budgets")

    def mutated(self, name, fn):
        s = Scratch()
        self.addCleanup(s.cleanup)
        d = s.path / "j"
        shutil.copytree(CASES / name / "files" / "j", d)
        fn(d)
        return self.join(name, d)

    def test_truncated_or_inconsistent_is_refused(self):
        def drop_entry(d):
            t = (d / "join.out").read_text().splitlines(keepends=True)
            k = next(i for i, l in enumerate(t) if l.startswith("no-bucket "))
            (d / "join.out").write_text("".join(t[:k] + t[k + 4:]))
        o = self.mutated("a2a-join-rust-b", drop_entry)
        self.assertEqual(o.reason, "unparsable")
        self.assertIn("uncovered: 5 but 4", o.message)

        def bad_counts(d):
            t = (d / "join.out").read_text().replace("violations=5 false_alarms=0", "violations=4 false_alarms=0")
            (d / "join.out").write_text(t)
        self.assertIn("!= uncovered", self.mutated("a2a-join-rust-b", bad_counts).message)

        def drop_row(d):
            t = (d / "graphs" / "index.tsv").read_text().splitlines(keepends=True)
            (d / "graphs" / "index.tsv").write_text("".join(t[:-1]))
        self.assertIn("4 rows for 5", self.mutated("a2a-join-rust-b", drop_row).message)

        def truncate_row(d):
            t = (d / "graphs" / "index.tsv").read_text()
            (d / "graphs" / "index.tsv").write_text(t[:-1])
        self.assertIn("truncated", self.mutated("a2a-join-rust-b", truncate_row).message)

        def no_summary(d):
            (d / "summary.txt").unlink()
        self.assertIn("no summary.txt", self.mutated("join-tf-bug", no_summary).message)

        def no_out(d):
            (d / "join.out").unlink()
        self.assertEqual(self.mutated("join-tf-bug", no_out).reason, "unparsable")

    def test_synthetic_decide_outcomes(self):
        """A Decide false alarm and an unresolved line (no S3 program plants them; the text is
        join-tf-bug's with the outcome words of report.rs:3140-3150)."""
        _, _, _, files = case("join-tf-bug")
        text = (files / "j" / "join.out").read_text()
        fa = text.replace("violations=1 false_alarms=0", "violations=0 false_alarms=1") \
                 .replace("  decide: violation k=0", "  decide: false-alarm k=2")
        rep = parse.conf_join(fa, 1)
        self.assertEqual((rep.decide["false_alarms"], rep.entries[0].decide["outcome"], rep.entries[0].decide["k"]),
                         (1, "false-alarm", 2))
        un = text.replace("violations=1 false_alarms=0 unresolved=0", "violations=0 false_alarms=0 unresolved=1") \
                 .replace("  decide: violation", "  decide: unresolved")
        self.assertEqual(parse.conf_join(un, 1).decide["unresolved"], 1)
        with self.assertRaisesRegex(parse.ParseError, "verdict violation with exit 0"):
            parse.conf_join(text, 0)


class Measurements(unittest.TestCase):
    def test_block_and_labels(self):
        _, out, _, _ = case("jmc-export-assert-spawned")
        before, block, after = parse.split_measurements(out)
        self.assertEqual(block["stop_reason"], "error")
        self.assertIn("FAILED:", after)
        lab = parse.labelled(out)
        self.assertEqual(lab["strategy"], "must")
        self.assertTrue(lab["FAILED"].startswith("must: process"))
        self.assertIsNone(parse.split_measurements("no block")[1])


if __name__ == "__main__":
    unittest.main()
