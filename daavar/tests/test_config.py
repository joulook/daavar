"""S2: Appendix A-B expand exactly as Appendix C lists; filter and validation errors name the valid choices."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import copy
import tomllib
import unittest

from daavar import config
from daavar.config import ConfigError
from daavar.tests.util import DAAVAR, FIXTURES

CONF = FIXTURES / "config"


def load(study):
    return config.load(CONF / f"{study}.daavar.toml")


def raw(study):
    with open(CONF / f"{study}.daavar.toml", "rb") as f:
        return tomllib.load(f)


def appendix_c():
    rows = []
    for line in (CONF / "appendix-c.md").read_text().splitlines():
        cols = [c.strip().strip("`") for c in line.strip().strip("|").split("|")]
        rows.append((cols[0], cols[1], cols[2], int(cols[3]), int(cols[4]), cols[5], cols[6]))
    return rows


def short(p, text):
    """Appendix C writes a Java class by its simple name (`A2ATask:impl,a`)."""
    if p.lang == "java":
        return text.replace(p.cls, p.cls.rsplit(".", 1)[-1], 1)
    return text


class AppendixC(unittest.TestCase):
    def test_every_row(self):
        rows = appendix_c()
        self.assertEqual(len(rows), 30)
        seen = []
        for study in ("a2a-task", "mcp-cancel"):
            c = load(study)
            for s in c.scenarios.values():
                for pid in s.programs:
                    p = c.programs[pid]
                    argv = [config.invocation(p, s, cell).argv_text for cell in s.cells if config.active(p, cell.label)]
                    for a in argv:
                        self.assertNotIn(",,", a)
                        self.assertFalse(a.endswith(",") or a.endswith(":"), a)
                    seen.append((study, s.id, pid, len(argv), len(set(argv)), short(p, argv[0]), short(p, argv[-1])))
        self.assertEqual(seen, rows)

    def test_rev37_mutants_skip_n(self):
        a = load("a2a-task")
        for n in ("n", "nrecancel"):
            (cell,) = config.select(a, scenarios=["one"], cells=[n])
            self.assertFalse([p for p in cell.invocations if p.startswith("mutant")])
            self.assertFalse([d for d in cell.directions if d.checked.startswith("mutant")])
        (cell,) = config.select(a, scenarios=["one"], cells=["b"])
        self.assertEqual([p for p in cell.invocations if p.startswith("mutant")],
                         ["mutant1", "mutant2", "mutant3", "mutant4"])

    def test_kv_cancel_example(self):
        """examples/kv-cancel/daavar.toml (phase 6) parses and expands."""
        c = config.load(DAAVAR / "examples" / "kv-cancel")
        sel = config.select(c)
        self.assertEqual([x.name for x in sel], ["one.impl", "one.mutant"])
        impl, mut = sel
        self.assertEqual({k: v.argv_text for k, v in impl.invocations.items()},
                         {"spec": "spec", "rust": "impl", "java": "bench.kv_cancel.KvCancel:impl"})
        self.assertEqual([d.name for d in impl.directions], ["rust<=spec", "java<=spec", "java<=rust", "rust<=java"])
        self.assertEqual({k: v.argv_text for k, v in mut.invocations.items()}, {"spec": "spec", "mutant1": "mutant1"})
        self.assertEqual([(d.name, d.form, d.gating) for d in mut.directions],
                         [("mutant1<=spec", "mutant<=reference", True)])

    def test_java_argument_is_the_full_class(self):
        c = load("a2a-task")
        s = c.scenarios["one"]
        self.assertEqual(config.invocation(c.programs["java"], s, s.cells[0]).argv_text,
                         "bench.a2a_task.A2ATask:impl,a")

    def test_labels(self):
        a = load("a2a-task")
        self.assertEqual([x.label for x in a.scenarios["one"].cells], ["a", "arecancel", "b", "brecancel", "n", "nrecancel"])
        deleg = [x.label for x in a.scenarios["deleg"].cells]
        self.assertEqual(deleg[:4], ["a", "b", "n", "n-local"])
        self.assertEqual(deleg[-1], "n-delegtask-ww")
        m = load("mcp-cancel")
        self.assertEqual(m.scenarios["pairs-c2"].cells[0].label, "aa.s0.c2")
        self.assertEqual(len(m.scenarios["pairs"].cells), 48)

    def test_directions(self):
        a = load("a2a-task")
        d = {x.name: x for x in a.scenarios["one"].directions}
        self.assertEqual((d["rust<=spec"].form, d["rust<=spec"].gating), ("impl<=spec", True))
        self.assertEqual((d["mutant1<=spec"].form, d["mutant1<=spec"].gating), ("mutant<=reference", True))
        self.assertEqual((d["java<=rust"].form, d["java<=rust"].gating), ("impl<=impl", False))
        self.assertEqual((d["mutant4<=spec"].form, d["mutant4<=spec"].gating), ("impl<=spec", True))

    def test_defaults_and_tools(self):
        a = load("a2a-task")
        self.assertEqual(a.check["decide_budget"], 1000000)
        self.assertFalse(a.check["stop_at_first"])
        self.assertEqual(a.budgets["java_xmx"], "16g")
        self.assertEqual(a.tools["jmc"]["cli"], "positional")
        self.assertEqual(a.vocab["opaque"], ("ErrCode",))
        self.assertIsNone(a.vocab["roots"])


class Selection(unittest.TestCase):
    def test_scenario_cell_and_field(self):
        a = load("a2a-task")
        sel = config.select(a, scenarios=["deleg"], where=[("split", ["local", "deleg"])])
        self.assertEqual([x.name for x in sel], ["deleg.n-local", "deleg.n-deleg"])
        sel = config.select(a, cells=["a"])
        self.assertEqual([x.name for x in sel], ["one.a", "deleg.a", "gated.a"])
        sel = config.select(a, where=[("variant", ["b"])])
        self.assertEqual([x.name for x in sel], ["one.b", "one.brecancel", "deleg.b", "gated.b"])

    def test_empty_value_filter(self):
        a = load("a2a-task")
        sel = config.select(a, scenarios=["one"], where=config.parse_where(["recancel="]))
        self.assertEqual([x.cell.label for x in sel], ["a", "b", "n"])

    def test_scenario_fields_are_filterable(self):
        m = load("mcp-cancel")
        sel = config.select(m, where=[("reading", ["s2"]), ("variant", ["ab"])])
        self.assertEqual([x.name for x in sel], ["pairs.ab.s2", "pairs-c2.ab.s2.c2"])

    def test_active_programs_and_directions(self):
        m = load("mcp-cancel")
        (c2,) = config.select(m, scenarios=["pairs-c2"], cells=["aa.s0.c2"])
        self.assertEqual(list(c2.invocations), ["spec", "rust", "mutant1", "mutant2", "mutant3"])
        self.assertEqual([d.name for d in c2.directions], ["rust<=spec", "mutant1<=spec", "mutant2<=spec", "mutant3<=spec"])

    def test_program_cells_limit_activity(self):
        r = raw("a2a-task")
        r["programs"]["java"]["cells"] = ["a"]
        c = config.from_dict(r, CONF)
        (one_b,) = config.select(c, scenarios=["one"], cells=["b"])
        self.assertNotIn("java", one_b.invocations)
        self.assertNotIn("java<=spec", [d.name for d in one_b.directions])
        self.assertNotIn("rust<=java", [d.name for d in one_b.directions])
        (one_a,) = config.select(c, scenarios=["one"], cells=["a"])
        self.assertIn("java<=spec", [d.name for d in one_a.directions])

    def test_unknown_names_list_valid_ones(self):
        a = load("a2a-task")
        with self.assertRaisesRegex(ConfigError, r"unknown scenario nope; valid: one, deleg, gated"):
            config.select(a, scenarios=["nope"])
        with self.assertRaisesRegex(ConfigError, r"unknown cell zz; valid: a, arecancel, b, brecancel, n, nrecancel$"):
            config.select(a, scenarios=["one"], cells=["zz"])
        with self.assertRaisesRegex(ConfigError, r"unknown field colour; valid: split, variant, recancel"):
            config.select(a, scenarios=["one"], where=[("colour", ["x"])])
        with self.assertRaisesRegex(ConfigError, r"unknown value 'q' of field variant; valid: a, b, n"):
            config.select(a, where=[("variant", ["q"])])
        with self.assertRaisesRegex(ConfigError, r"unknown value 'local' of field split; valid: \"\""):
            config.select(a, scenarios=["one"], where=[("split", ["local"])])
        with self.assertRaisesRegex(ConfigError, "select no cell"):
            config.select(a, scenarios=["one"], where=[("variant", ["a"]), ("recancel", ["recancel"]),
                                                       ("variant", ["b"])])
        with self.assertRaisesRegex(ConfigError, "expected <field>=<values>"):
            config.parse_where(["variant"])


class Validation(unittest.TestCase):
    def bad(self, mutate, pattern, study="a2a-task"):
        r = copy.deepcopy(raw(study))
        mutate(r)
        with self.assertRaisesRegex(ConfigError, pattern):
            config.from_dict(r, CONF)

    def test_unresolved_template(self):
        def m(r):
            r["programs"]["spec"]["args"] = ["{fam.spec}", "{colour}"]
        self.bad(m, r"\{colour\} does not resolve")

    def test_unknown_table_key(self):
        def m(r):
            r["programs"]["spec"]["args"] = ["{fam.nope}"]
        self.bad(m, r"table fam has no key nope; keys: spec, impl, implf")

    def test_unbalanced(self):
        def m(r):
            r["programs"]["spec"]["args"] = ["{fam.spec"]
        self.bad(m, "unbalanced brace")

    def test_direction_unknown_program(self):
        def m(r):
            r["scenarios"]["one"]["directions"].append("ghost<=spec")
        self.bad(m, "ghost is not a program of this scenario")

    def test_mutant_against_other_reference(self):
        def m(r):
            r["scenarios"]["one"]["directions"].append("mutant1<=rust")
        self.bad(m, "checked against its declared reference spec only")

    def test_mutant_reference_missing(self):
        def m(r):
            r["programs"]["mutant1"]["reference"] = "nobody"
        self.bad(m, "'nobody' names no program")

    def test_spec_checked(self):
        def m(r):
            r["scenarios"]["one"]["directions"].append("spec<=rust")
        self.bad(m, r"spec<=impl is not a direction")

    def test_causal_rust_only(self):
        def m(r):
            r["model"] = "causal"
        self.bad(m, "causal is Rust-only")

    def test_causal_ok_without_java(self):
        r = copy.deepcopy(raw("mcp-cancel"))
        r["model"] = "causal"
        del r["programs"]["java"]
        r["scenarios"]["pairs"]["programs"].remove("java")
        r["scenarios"]["pairs"]["directions"] = ["rust<=spec"]
        config.from_dict(r, CONF)

    def test_duplicate_labels(self):
        def m(r):
            r["scenarios"]["one"]["matrix"]["label"] = "{variant}"
        self.bad(m, "label 'a' is not distinct")

    def test_unknown_keys(self):
        self.bad(lambda r: r.__setitem__("colour", 1), r"unknown key\(s\) colour")
        self.bad(lambda r: r["programs"]["spec"].__setitem__("sides", "x"), r"programs.spec: unknown key")
        self.bad(lambda r: r["check"].__setitem__("budget", 1), r"check: unknown key")

    def test_rust_needs_side_java_needs_class(self):
        self.bad(lambda r: r["programs"]["spec"].pop("side"), "needs side")
        self.bad(lambda r: r["programs"]["java"].pop("class"), "needs a class")

    def test_inactive_cells_not_expanded(self):
        r = copy.deepcopy(raw("a2a-task"))
        r["programs"]["java"]["cells"] = ["a"]
        r["programs"]["java"]["args"] = ["{fam.impl}", "{variant}", "{only_a}"]
        r["scenarios"]["one"]["cells"] = [{"label": "a", "variant": "a", "recancel": "", "only_a": "x"}]
        r["scenarios"]["one"]["matrix"] = {"variant": ["b"], "recancel": [""], "label": "{variant}"}
        for k in (1, 2, 3, 4):
            r["programs"][f"mutant{k}"].pop("cells")
        for sc in ("deleg", "gated"):
            r["scenarios"][sc]["programs"].remove("java")
            r["scenarios"][sc]["directions"] = [d for d in r["scenarios"][sc]["directions"] if "java<" not in d
                                                and "<=java" not in d]
        c = config.from_dict(r, CONF)
        (a,) = config.select(c, scenarios=["one"], cells=["a"])
        self.assertEqual(a.invocations["java"].argv_text, "bench.a2a_task.A2ATask:impl,a,x")

    def test_program_cells_unknown_label(self):
        self.bad(lambda r: r["programs"]["java"].__setitem__("cells", ["zz"]), r"unknown label\(s\) zz")

    def test_model(self):
        self.bad(lambda r: r.__setitem__("model", "p2p"), "'p2p' is not one of fifo, bag, causal")

    def test_jmc_cli(self):
        self.bad(lambda r: r["tools"]["jmc"].__setitem__("cli", "x"), "is not one of positional, named, auto")

    def test_missing_jmc_with_java(self):
        self.bad(lambda r: r["tools"].pop("jmc"), r"missing \[tools.jmc\]")


if __name__ == "__main__":
    unittest.main()
