"""S5: vocab gives 0 on both studies and on a single-language project, 1 on planted defects, 2 when unparsable."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import shutil
import unittest

from daavar import config, vocab
from daavar.tests.util import FIXTURES, Scratch

V = FIXTURES / "vocab"


def run(project, values=False):
    return vocab.run(config.load(project), values=values)


class Studies(unittest.TestCase):
    def test_both_studies_identical(self):
        for study, n in (("a2a-task", 19), ("mcp-cancel", 10)):
            for values in (False, True):
                with self.subTest(study=study, values=values):
                    r = run(V / study, values)
                    self.assertEqual((r.status, r.exit), ("IDENTICAL", 0), r.differences)
                    self.assertEqual(len(r.rust_tags), n)

    def test_roots_derived(self):
        a = vocab.parse_rust((V / "a2a-task/rust/src/msgs.rs").read_text())
        self.assertEqual(vocab.rust_roots(a), ["ToServer", "ToClient"])
        self.assertEqual(a.custom_debug, {"ErrCode"})
        self.assertEqual(vocab.payload_enums(a, ["ToServer", "ToClient"], {"ErrCode"}), ["Code", "State"])
        m = vocab.parse_rust((V / "mcp-cancel/rust/src/msgs.rs").read_text())
        # Branch is used only by the struct Args: payload, not a root (DRIVER-IMPL-NOTES D5).
        self.assertEqual(vocab.rust_roots(m), ["ToServer", "ToClient", "ToHandler"])

    def test_each_java_class_on_its_own(self):
        r = run(V / "a2a-task")
        self.assertEqual(sorted(r.java_tags), ["java", "java-fixed"])
        src = """package p;
            public final class Outer {
              public static final class A { @Tag("X") record X(int a) {} }
              public static final class B { @Tag("Y") record Y() {} enum E { K } }
            }
            final class Sibling { @Tag("Z") record Z(int a, int b) {} }"""
        a = vocab.parse_java(src, "Outer.java", ["Outer", "A"])
        b = vocab.parse_java(src, "Outer.java", ["Outer", "B"])
        self.assertEqual(([t for t, _, _ in a.tags], [t for t, _, _ in b.tags]), (["X"], ["Y"]))
        self.assertEqual((a.enums, b.enums), ({}, {"E": ["K"]}))
        self.assertEqual([t for t, _, _ in vocab.parse_java(src, "Outer.java", ["Outer"]).tags], ["X", "Y"])
        self.assertEqual([t for t, _, _ in vocab.parse_java(src, "Outer.java", ["Sibling"]).tags], ["Z"])
        with self.assertRaises(vocab.VocabError):
            vocab.parse_java(src, "Outer.java", ["Nope"])
        # Through the configured program name (MpLauncher's binary name p.Outer$A, or p.Outer.A).
        s = Scratch()
        self.addCleanup(s.cleanup)
        (s.path / "java" / "p").mkdir(parents=True)
        (s.path / "java" / "p" / "Outer.java").write_text(src)
        for name in ("p.Outer$A", "p.Outer.A"):
            f, inner = vocab.java_source(s.path, name)
            self.assertEqual((f.name, inner), ("Outer.java", ["Outer", "A"]))
            self.assertEqual([t for t, _, _ in vocab.parse_java(f.read_text(), str(f), inner).tags], ["X"])

    def test_fx(self):
        r = run(V / "fx", values=True)
        self.assertEqual((r.status, r.exit), ("IDENTICAL", 0), r.differences)
        self.assertEqual(r.rust_tags, {"Ping": 1, "Shutdown": 0, "Pong": 1})

    def test_single_language(self):
        c = config.load(V / "mcp-cancel")
        rust_only = {k: v for k, v in c.programs.items() if v.lang == "rust"}
        c.programs = rust_only
        r = vocab.run(c)
        self.assertEqual((r.status, r.exit), ("ONE-SIDED", 0))
        self.assertIn("one side only", r.notes[0])


class Planted(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.d = self.s.path / "p"
        shutil.copytree(V / "a2a-task", self.d)

    def tearDown(self):
        self.s.cleanup()

    def edit(self, rel, old, new):
        p = self.d / rel
        t = p.read_text()
        self.assertIn(old, t)
        p.write_text(t.replace(old, new, 1))

    JAVA = "java/bench/a2a_task/A2ATask.java"
    MSGS = "rust/src/msgs.rs"

    def kinds(self, values=False):
        r = run(self.d, values)
        return r, sorted({(p, k) for p, k, _ in r.differences})

    def test_arity(self):
        self.edit(self.JAVA, "public record Get(int req, String task)", "public record Get(int req)")
        r, k = self.kinds()
        self.assertEqual((r.status, r.exit, k), ("DIFFERENT", 1, [("java", "arity")]))
        self.assertIn("tag Get: Rust arity 2, Java arity 1", r.differences[0][2])

    def test_one_sided_java(self):
        self.edit(self.JAVA, '@Tag("Reply")', '@Tag("Answer")')
        r, k = self.kinds()
        self.assertEqual(r.exit, 1)
        texts = [t for _, _, t in r.differences]
        self.assertIn("tag Reply (arity 1) is in Rust only", texts)
        self.assertIn("tag Answer (arity 1) is in Java only", texts)

    def test_one_sided_allowed(self):
        self.edit(self.JAVA, '@Tag("Reply")', '@Tag("Answer")')
        self.edit("daavar.toml", 'opaque = ["ErrCode"]', 'opaque = ["ErrCode"]\nrust_only = ["Reply"]\njava_only = ["Answer"]')
        r, _ = self.kinds()
        self.assertEqual((r.status, r.exit), ("IDENTICAL", 0), r.differences)

    def test_two_arities_rust(self):
        self.edit(self.MSGS, "    /// The server ends: the executor ends too.\n    Shutdown,",
                  "    Shutdown { why: u32 },")
        r, k = self.kinds()
        self.assertIn(("rust", "two-arities"), k)
        self.assertEqual(r.exit, 1)

    def test_values(self):
        self.edit(self.JAVA, "public enum State { Submitted, Working, Completed, Canceled }",
                  "public enum State { Submitted, Working, Done, Canceled }")
        r, k = self.kinds()
        self.assertEqual(r.exit, 0)  # tags and arity agree
        r, k = self.kinds(values=True)
        self.assertEqual((r.exit, k), (1, [("java", "values")]))  # A2ATaskFixed is unchanged

    def test_values_order_free(self):
        self.edit(self.JAVA, "public enum State { Submitted, Working, Completed, Canceled }",
                  "public enum State { Canceled, Completed, Working, Submitted }")
        r, k = self.kinds(values=True)
        self.assertEqual(r.exit, 0, r.differences)

    def test_roots_override(self):
        self.edit("daavar.toml", 'opaque = ["ErrCode"]', 'opaque = ["ErrCode"]\nroots = ["ToServer"]')
        r, k = self.kinds()
        self.assertEqual(r.exit, 1)
        self.assertIn("tag TaskItem (arity 3) is in Java only", [t for _, _, t in r.differences])

    def test_unparsable(self):
        self.edit(self.MSGS, "pub enum ToServer {", "pub enum ToServer {{")
        r = run(self.d)
        self.assertEqual((r.status, r.exit), ("UNPARSABLE", 2))
        (self.d / self.MSGS).unlink()
        r = run(self.d)
        self.assertEqual((r.status, r.exit), ("UNPARSABLE", 2))
        self.assertIn("msgs.rs", r.notes[0])

    def test_java_escapes_decoded(self):
        j = vocab.parse_java('class C { @Tag("\\120ing") record P(int n) {} @Tag("\\u0050ong") record Q(int n) {} '
                             '@Tag("\\u005c120ingX") record R(int n) {} @Tag("a\\\\u0050") record S() {} }')
        self.assertEqual([t for t, _, _ in j.tags], ["Ping", "Pong", "PingX", "a\\u0050"])
        with self.assertRaises(vocab.VocabError):
            vocab.parse_java('class C { @Tag("\\q") record P() {} }')

    def test_text_block_tag_unparsable(self):
        with self.assertRaises(vocab.VocabError):
            vocab.parse_java('class C { @Tag("""\n    Ping""") record P(int n) {} }')

    def test_tag_expression_unparsable(self):
        self.edit(self.JAVA, '@Tag("Reply")', '@Tag(value = "Reply" + SUFFIX)')
        self.assertEqual(run(self.d).exit, 2)

    def test_tag_not_on_a_record(self):
        self.edit(self.JAVA, '@Tag("Shutdown")\n    public record Shutdown()', '@Tag("Shutdown")\n    public final class Shutdown')
        r = run(self.d)
        self.assertEqual(r.exit, 2)


class Tokenizer(unittest.TestCase):
    def test_comments_strings_lifetimes(self):
        m = vocab.parse_rust('''
            /* a /* nested */ comment with enum Fake { X } */
            // enum Fake2 { Y }
            pub enum A<'a> { #[doc = "x { , }"] P { s: &'a str, t: Vec<(u8, u8)> }, Q(u8, B), R = 3, }
            pub enum B { C, D }
            impl<'a> core::fmt::Debug for A<'a> { fn fmt(&self) {} }
        ''')
        self.assertEqual(list(m.enums), ["A", "B"])
        self.assertEqual(m.enums["A"].variants, {"P": 2, "Q": 2, "R": 0})
        self.assertEqual(vocab.rust_roots(m), ["A"])
        self.assertEqual(m.custom_debug, {"A"})

    def test_unicode_identifiers(self):
        m = vocab.parse_rust("pub enum Msg { Píng(u32), Pong }")
        self.assertEqual(m.enums["Msg"].variants, {"Píng": 1, "Pong": 0})

    def test_conditional_variant_unparsable(self):
        with self.assertRaises(vocab.VocabError):
            vocab.parse_rust("pub enum Msg { Ping, #[cfg(any())] Hidden }")

    def test_raw_identifier_unparsable(self):
        with self.assertRaises(vocab.VocabError):
            vocab.parse_rust("pub enum Msg { r#type(u32) }")

    def test_java(self):
        j = vocab.parse_java('''
            /** {@code @Tag("Nope")} */
            class X {
              @Tag(value = "V") @Deprecated public static record V(java.util.List<java.util.Map<String, Integer>> a, int b) implements M {}
              @org.mpi_sws.jmc.mp.summary.Tag("W") record W() {}
              enum E { A(1), B { }, C; void f() {} }
              String s = "@Tag(\\"Z\\") record Z(int a)";
            }
        ''')
        self.assertEqual([(t, a) for t, a, _ in j.tags], [("V", 2), ("W", 0)])
        self.assertEqual(j.enums["E"], ["A", "B", "C"])


if __name__ == "__main__":
    unittest.main()
