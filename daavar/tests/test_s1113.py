"""S11, S13 units: scaffold naming, refusals and outputs; replay's standalone resolution and refusals."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import json
import os
import tomllib
import unittest
from unittest import mock

from daavar import DaavarError, config, replay, scaffold, vocab
from daavar.tests.util import FIXTURES, Scratch


class Scaffold(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()

    def tearDown(self):
        self.s.cleanup()

    def test_names(self):
        self.assertEqual(scaffold.camel("kv-cancel"), "KvCancel")
        for bad in ("Kv", "1x", "a_b", "-a", ""):
            with self.assertRaisesRegex(DaavarError, "must match"):
                scaffold.new(bad, dest=self.s.path / "x")

    def test_java_spec_refused(self):
        with self.assertRaisesRegex(DaavarError, "release 1 does not scaffold one"):
            scaffold.new("x", spec="java", dest=self.s.path / "x")

    def test_each_impl_choice(self):
        for impl, dirs in (("rust", ["rust<=spec"]), ("java", ["java<=spec"]),
                           ("rust,java", ["rust<=spec", "java<=spec", "java<=rust", "rust<=java"])):
            with self.subTest(impl=impl):
                d = scaffold.new("my-echo", impl, dest=self.s.path / impl / "with space" / "my-echo", warn=lambda m: None)
                cfg = config.load(d)
                (cell,) = config.select(cfg)
                self.assertEqual([x.name for x in cell.directions], dirs)
                ct = tomllib.loads((d / "rust" / "Cargo.toml").read_text())
                self.assertEqual(ct["package"]["name"], "tf_my_echo")
                dep = ct["dependencies"]["traceforge"]["path"]
                self.assertTrue((d / "rust" / dep / "Cargo.toml").is_file(), dep)
                self.assertIn('name = "tf_my_echo"', (d / "rust" / "Cargo.lock").read_text())
                if "java" in impl:
                    self.assertTrue((d / "java" / "bench" / "my_echo" / "MyEcho.java").is_file())
                    r = vocab.run(cfg, values=True)
                    self.assertEqual((r.status, r.exit), ("IDENTICAL", 0), r.differences)

    def test_non_empty_dir_refused(self):
        (self.s.path / "x").mkdir()
        (self.s.path / "x" / "f").write_text("")
        with self.assertRaisesRegex(DaavarError, "not empty"):
            scaffold.new("x", dest=self.s.path / "x")


class Replay(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.env = mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(self.s.path / "cache")})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.s.cleanup()

    def graph(self):
        g = FIXTURES / "cases" / "join-tf-bug" / "files" / "j" / "graphs" / "1.graph.json"
        return g, replay._graph(g)

    def test_standalone_needs_one_artifact(self):
        p, g = self.graph()
        with self.assertRaisesRegex(DaavarError, "no cached artifact matches"):
            replay.resolve(p, g)
        src = g["run"]["source"]  # "unknown" in this fixture (no manifest beside fx): any manifest matches
        files = src["files"] if isinstance(src, dict) else {}
        man = "# root x\n" + "".join(f"{v}  {k}\n" for k, v in sorted(files.items()))
        for k in ("a" * 64, "b" * 64):
            d = self.s.path / "cache" / "artifacts" / k
            d.mkdir(parents=True)
            (d / "build.json").write_text(json.dumps({"key": k, "manifest": man, "sha256": {},
                                                      "key_inputs": {"kind": "rust", "tool": g["run"]["tool_commit"],
                                                                     "bins": [g["run"]["entry"]]}}))
            if k == "a" * 64:
                self.assertEqual(replay.resolve(p, g)["artifact"]["key"], k)
        with self.assertRaisesRegex(DaavarError, "2 cached artifacts match"):
            replay.resolve(p, g)

    def test_not_a_graph(self):
        p = self.s.path / "x.json"
        p.write_text('{"a":1}\n')
        with self.assertRaisesRegex(DaavarError, "not a witness graph"):
            replay._graph(p)


if __name__ == "__main__":
    unittest.main()
