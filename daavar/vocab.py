"""`daavar vocab` (DRIVER-DESIGN §3.4): message tags and field arity of the Rust and Java programs compared.

Rust: the tags are the variants of the root enums of `rust/src/msgs.rs`, an enum being a root when no
enum variant and no struct field uses it as a field type (so A2A's `State` and `Code`, and MCP's
`Branch`, are payload); `[vocab] roots` overrides the derivation. Java: the `@Tag` records of each class's
source file, each class compared on its own against the Rust set.

Differences: a one-sided tag (unless listed in `rust_only` / `java_only`), an arity mismatch, a tag with two
arities on one side. `--values` also compares the constants of the payload enums reachable from the roots,
skipping types with a hand-written `Debug` (`ErrCode`) and `[vocab] opaque` types.
Exit 0 identical or one-sided, 1 different, 2 unparsable.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from . import DaavarError


class VocabError(DaavarError):
    """A source the parser cannot read (exit 2)."""


# ---------------------------------------------------------------- a small tokenizer for Rust and Java

_IDENT = re.compile(r"(?:[^\W\d]|\$)[\w$]*")  # Unicode identifiers (Rust and Java both allow them)
_NUM = re.compile(r"[0-9][A-Za-z0-9_.]*")


@dataclass(frozen=True)
class Tok:
    kind: str  # id | str | num | p (punctuation) | life (a Rust lifetime)
    text: str


def _java_unicode(src: str, where: str) -> str:
    """JLS §3.3: \\uXXXX (any number of u) is translated before tokenizing, when its backslash is preceded by
    an even number of backslashes."""
    if "\\u" not in src:
        return src
    out, i, n = [], 0, len(src)
    while i < n:
        c = src[i]
        if c == "\\":
            k = i
            while k < n and src[k] == "\\":
                k += 1
            run = k - i
            if k < n and src[k] == "u" and run % 2 == 1:
                out.append("\\" * (run - 1))
                j = k
                while j < n and src[j] == "u":
                    j += 1
                hx = src[j:j + 4]
                if len(hx) != 4 or any(x not in "0123456789abcdefABCDEF" for x in hx):
                    raise VocabError(f"{where}: bad unicode escape")
                out.append(chr(int(hx, 16)))
                i = j + 4
                continue
            out.append(src[i:k])
            i = k
            continue
        out.append(c)
        i += 1
    return "".join(out)


def tokenize(src: str, lang: str, where: str = "") -> list:
    if lang == "java":
        src = _java_unicode(src, where)
    out = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
            continue
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        if src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if lang == "rust" and src.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif src.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                raise VocabError(f"{where}: unterminated comment")
            i = j
            continue
        if lang == "java" and src.startswith('"""', i):
            j = src.find('"""', i + 3)
            if j < 0:
                raise VocabError(f"{where}: unterminated text block")
            out.append(Tok("textblock", src[i + 3:j]))  # not decoded: refused where a value matters
            i = j + 3
            continue
        if lang == "rust":
            m = re.match(r'b?r(#*)"', src[i:])
            if m:
                hashes = m.group(1)
                start = i + m.end()
                j = src.find('"' + hashes, start)
                if j < 0:
                    raise VocabError(f"{where}: unterminated raw string")
                out.append(Tok("str", src[start:j]))
                i = j + 1 + len(hashes)
                continue
            if src.startswith('b"', i):
                i += 1
                c = '"'
        if c == '"':
            j, buf = i + 1, []
            while j < n and src[j] != '"':
                if src[j] == "\\" and j + 1 < n:
                    buf.append(src[j:j + 2])
                    j += 2
                else:
                    buf.append(src[j])
                    j += 1
            if j >= n:
                raise VocabError(f"{where}: unterminated string")
            text = "".join(buf)
            out.append(Tok("str", _java_unescape(text, where) if lang == "java" else text))
            i = j + 1
            continue
        if c == "'":
            if i + 1 < n and src[i + 1] == "\\":
                j = src.find("'", i + 2)
                if j < 0:
                    raise VocabError(f"{where}: unterminated character literal")
                out.append(Tok("str", src[i + 1:j]))
                i = j + 1
                continue
            if i + 2 < n and src[i + 2] == "'":
                out.append(Tok("str", src[i + 1]))
                i += 3
                continue
            m = _IDENT.match(src, i + 1)
            if lang == "rust" and m:
                out.append(Tok("life", src[i:m.end()]))
                i = m.end()
                continue
            raise VocabError(f"{where}: bad character literal")
        if lang == "rust" and src.startswith("r#", i) and _IDENT.match(src, i + 2):
            raise VocabError(f"{where}: a raw identifier (r#...) is not read; rename it")
        m = _IDENT.match(src, i)
        if m:
            out.append(Tok("id", m.group(0)))
            i = m.end()
            continue
        m = _NUM.match(src, i)
        if m:
            out.append(Tok("num", m.group(0)))
            i = m.end()
            continue
        for p in ("->", "=>", "::"):
            if src.startswith(p, i):
                out.append(Tok("p", p))
                i += 2
                break
        else:
            out.append(Tok("p", c))
            i += 1
    return out


_JAVA_ESC = {"b": "\b", "t": "\t", "n": "\n", "f": "\f", "r": "\r", "s": " ", '"': '"', "'": "'", "\\": "\\"}


def _java_unescape(s: str, where: str) -> str:
    """A Java string literal's value after the unicode pre-pass: octal and the single-character escapes."""
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        if i + 1 >= len(s):
            raise VocabError(f"{where}: bad escape in a string literal")
        d = s[i + 1]
        if d in "01234567":
            j = i + 1
            lim = 3 if d in "0123" else 2
            while j < len(s) and j - (i + 1) < lim and s[j] in "01234567":
                j += 1
            out.append(chr(int(s[i + 1:j], 8)))
            i = j
        elif d in _JAVA_ESC:
            out.append(_JAVA_ESC[d])
            i += 2
        else:
            raise VocabError(f"{where}: unknown escape \\{d} in a string literal")
    return "".join(out)


_OPEN = {"(": ")", "[": "]", "{": "}", "<": ">"}
_CLOSE = {v: k for k, v in _OPEN.items()}


def _match(toks: list, i: int, where: str) -> int:
    """The index of the bracket closing the one at toks[i] (round, square, curly; angle counted inside)."""
    stack = [toks[i].text]
    j = i + 1
    while j < len(toks):
        t = toks[j]
        if t.kind == "p":
            if t.text in ("(", "[", "{"):
                stack.append(t.text)
            elif t.text in (")", "]", "}"):
                if not stack or stack[-1] != _CLOSE[t.text]:
                    raise VocabError(f"{where}: unbalanced {t.text}")
                stack.pop()
                if not stack:
                    return j
        j += 1
    raise VocabError(f"{where}: unbalanced {toks[i].text}")


def _split(toks: list, sep: str = ",") -> list:
    """Top-level parts of a token list, split at `sep` (brackets and angle brackets nest)."""
    parts, cur, depth, angle = [], [], 0, 0
    for t in toks:
        if t.kind == "p":
            if t.text in ("(", "[", "{"):
                depth += 1
            elif t.text in (")", "]", "}"):
                depth -= 1
            elif t.text == "<":
                angle += 1
            elif t.text == ">" and angle:
                angle -= 1
            elif t.text == sep and depth == 0 and angle == 0:
                parts.append(cur)
                cur = []
                continue
        cur.append(t)
    if cur:
        parts.append(cur)
    return [p for p in parts if p]


def _strip_attrs(toks: list, lang: str) -> list:
    """Leading attributes (`#[...]`) or annotations (`@X`, `@X(...)`), and visibility (`pub(crate)`)."""
    i = 0
    while i < len(toks):
        t = toks[i]
        if lang == "rust" and t.text == "#" and i + 1 < len(toks) and toks[i + 1].text in ("[", "!"):
            k = i + 1 if toks[i + 1].text == "[" else i + 2
            i = _match(toks, k, "attribute") + 1
        elif lang == "java" and t.text == "@" and i + 1 < len(toks) and toks[i + 1].kind == "id" \
                and toks[i + 1].text != "interface":
            i += 2
            while i + 1 < len(toks) and toks[i].text == "." and toks[i + 1].kind == "id":
                i += 2
            if i < len(toks) and toks[i].text == "(":
                i = _match(toks, i, "annotation") + 1
        elif lang == "rust" and t.text == "pub":
            i += 1
            if i < len(toks) and toks[i].text == "(":
                i = _match(toks, i, "visibility") + 1
        else:
            break
    return toks[i:]


def _idents(toks: list) -> set:
    return {t.text for t in toks if t.kind == "id"}


# ---------------------------------------------------------------- Rust


@dataclass
class RustEnum:
    name: str
    variants: dict  # name -> arity, in order
    field_types: set  # identifiers used in the variants' field types


@dataclass
class RustModule:
    path: str
    enums: dict  # name -> RustEnum
    struct_field_types: set
    custom_debug: set


def parse_rust(src: str, where: str = "msgs.rs") -> RustModule:
    toks = tokenize(src, "rust", where)
    for k in range(len(toks) - 2):
        if toks[k].text == "#" and toks[k + 1].text in ("[", "!") and \
                any(t.kind == "id" and t.text in ("cfg", "cfg_attr") for t in toks[k + 1:k + 4]):
            raise VocabError(f"{where}: a conditional item (#[cfg]/#[cfg_attr]) is not read; the vocabulary must "
                             f"not depend on the build configuration")
    enums, sft, dbg = {}, set(), set()
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.kind == "id" and t.text in ("enum", "struct") and i + 1 < len(toks) and toks[i + 1].kind == "id":
            kind, name = t.text, toks[i + 1].text
            j = i + 2
            # generics and where clauses: skip to the body
            while j < len(toks) and toks[j].text not in ("{", "(", ";"):
                j += 1
            if j >= len(toks):
                raise VocabError(f"{where}: {kind} {name} has no body")
            if toks[j].text == ";":
                if kind == "enum":
                    raise VocabError(f"{where}: enum {name} has no body")
                i = j + 1
                continue
            end = _match(toks, j, f"{where}: {kind} {name}")
            body = toks[j + 1:end]
            if kind == "struct":
                for part in _split(body):
                    part = _strip_attrs(part, "rust")
                    if toks[j].text == "{":
                        k = next((x for x, tk in enumerate(part) if tk.text == ":"), None)
                        sft |= _idents(part[k + 1:] if k is not None else part)
                    else:
                        sft |= _idents(part)
            else:
                variants, ft = {}, set()
                for part in _split(body):
                    part = _strip_attrs(part, "rust")
                    if not part or part[0].kind != "id":
                        raise VocabError(f"{where}: enum {name}: cannot read a variant")
                    vname = part[0].text
                    rest = part[1:]
                    if rest and rest[0].text in ("{", "("):
                        close = _match(rest, 0, f"{where}: {name}::{vname}")
                        inner = rest[1:close]
                        fields = _split(inner)
                        if rest[0].text == "{":
                            for f in fields:
                                f = _strip_attrs(f, "rust")
                                k = next((x for x, tk in enumerate(f) if tk.text == ":"), None)
                                if k is None:
                                    raise VocabError(f"{where}: {name}::{vname}: a field without a type")
                                ft |= _idents(f[k + 1:])
                        else:
                            for f in fields:
                                ft |= _idents(_strip_attrs(f, "rust"))
                        arity = len(fields)
                    else:
                        arity = 0
                    if vname in variants:
                        raise VocabError(f"{where}: enum {name}: variant {vname} twice")
                    variants[vname] = arity
                enums[name] = RustEnum(name, variants, ft)
            i = end + 1
            continue
        if t.kind == "id" and t.text == "impl":
            # impl [<..>] [path::]Debug for NAME
            j = i + 1
            seg = []
            while j < len(toks) and toks[j].text not in ("{", ";"):
                seg.append(toks[j])
                j += 1
            texts = [x.text for x in seg]
            if "for" in texts:
                k = texts.index("for")
                trait = [x for x in texts[:k] if x not in ("::", "<", ">")]
                if trait and trait[-1] == "Debug" and k + 1 < len(texts):
                    dbg.add(texts[k + 1])
            i = j
            continue
        i += 1
    return RustModule(where, enums, sft, dbg)


def rust_roots(m: RustModule) -> list:
    used = set(m.struct_field_types)
    for e in m.enums.values():
        used |= e.field_types
    return [n for n in m.enums if n not in used]


# ---------------------------------------------------------------- Java

_JAVA_MODIFIERS = ("public", "protected", "private", "static", "final", "abstract", "strictfp", "sealed", "non")


@dataclass
class JavaClass:
    path: str
    tags: list  # (tag, arity, record name) in order
    enums: dict  # name -> [constants]


def _class_tokens(toks: list, simple: str, where: str) -> list:
    """The tokens of the declaration of type `simple` (class, interface, enum or record), body included."""
    for k, t in enumerate(toks):
        if t.kind == "id" and t.text in ("class", "interface", "enum", "record") and k + 1 < len(toks) \
                and toks[k + 1].text == simple and (k == 0 or toks[k - 1].text != "."):
            j = k + 2
            while j < len(toks) and toks[j].text != "{":
                j += 1
            if j >= len(toks):
                break
            return toks[k:_match(toks, j, f"{where}: {simple}") + 1]
    raise VocabError(f"{where}: no declaration of {simple}")


def parse_java(src: str, where: str = "X.java", cls: str | None = None) -> JavaClass:
    """The @Tag records and enums of the file, or with `cls` (a class name, nested parts after the file's
    outer class) of that class's declaration only."""
    toks = tokenize(src, "java", where)
    if cls:
        for part in cls:
            toks = _class_tokens(toks, part, where)
    tags, enums = [], {}
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.text == "@" and i + 1 < len(toks) and toks[i + 1].kind == "id":
            # @Tag("X"), @Tag(value = "X"), @org...summary.Tag("X")
            j = i + 1
            path = [toks[j].text]
            while j + 2 < len(toks) and toks[j + 1].text == "." and toks[j + 2].kind == "id":
                j += 2
                path.append(toks[j].text)
            if path[-1] == "Tag" and j + 1 < len(toks) and toks[j + 1].text == "(":
                close = _match(toks, j + 1, f"{where}: @Tag")
                inner = toks[j + 2:close]
                strs = [x for x in inner if x.kind == "str"]
                forms_ok = (len(inner) == 1 and inner[0].kind == "str") or \
                    (len(inner) == 3 and [x.text for x in inner[:2]] == ["value", "="] and inner[2].kind == "str")
                if len(strs) != 1 or not forms_ok:
                    raise VocabError(f"{where}: cannot read the @Tag at token {i} (only @Tag(\"X\") and "
                                     f"@Tag(value = \"X\") with one string literal are read)")
                tag = strs[0].text
                # Between the annotation and `record`: modifiers and other annotations only.
                k = close + 1
                while k < len(toks):
                    if toks[k].text == "@":
                        k = len(toks) - len(_strip_attrs(toks[k:], "java"))
                    elif toks[k].kind == "id" and toks[k].text in _JAVA_MODIFIERS:
                        k += 1
                    else:
                        break
                if k + 2 >= len(toks) or toks[k].text != "record" or toks[k + 1].kind != "id" \
                        or toks[k + 2].text not in ("(", "<"):
                    raise VocabError(f"{where}: @Tag({tag!r}) is not on a record")
                rname = toks[k + 1].text
                p = k + 2
                if toks[p].text == "<":  # generic record: skip the type parameters
                    depth = 0
                    while p < len(toks):
                        if toks[p].text == "<":
                            depth += 1
                        elif toks[p].text == ">":
                            depth -= 1
                            if depth == 0:
                                p += 1
                                break
                        p += 1
                if p >= len(toks) or toks[p].text != "(":
                    raise VocabError(f"{where}: record {rname} has no components")
                pclose = _match(toks, p, f"{where}: record {rname}")
                comps = _split(toks[p + 1:pclose])
                tags.append((tag, len(comps), rname))
                i = pclose + 1
                continue
            i = j + 1
            continue
        if t.kind == "id" and t.text == "enum" and i + 1 < len(toks) and toks[i + 1].kind == "id":
            name = toks[i + 1].text
            j = i + 2
            while j < len(toks) and toks[j].text != "{":
                j += 1
            if j >= len(toks):
                raise VocabError(f"{where}: enum {name} has no body")
            end = _match(toks, j, f"{where}: enum {name}")
            body = toks[j + 1:end]
            semi = next((x for x, tk in enumerate(body) if tk.text == ";" and _depth0(body, x)), len(body))
            consts = []
            for part in _split(body[:semi]):
                part = _strip_attrs(part, "java")
                if part and part[0].kind == "id":
                    consts.append(part[0].text)
            enums[name] = consts
            i = j + 1  # nested declarations inside the enum body are read too
            continue
        i += 1
    return JavaClass(where, tags, enums)


def _depth0(toks: list, idx: int) -> bool:
    d = 0
    for t in toks[:idx]:
        if t.text in ("(", "[", "{"):
            d += 1
        elif t.text in (")", "]", "}"):
            d -= 1
    return d == 0


# ---------------------------------------------------------------- comparison


@dataclass
class Report:
    status: str  # IDENTICAL | ONE-SIDED | DIFFERENT | UNPARSABLE
    exit: int
    differences: list = field(default_factory=list)  # (program, kind, text)
    notes: list = field(default_factory=list)
    rust_tags: dict = field(default_factory=dict)
    java_tags: dict = field(default_factory=dict)  # program -> {tag: arity}

    def as_dict(self) -> dict:
        return {"status": self.status, "exit": self.exit,
                "differences": [{"program": p, "kind": k, "text": t} for p, k, t in self.differences],
                "notes": self.notes, "rust_tags": self.rust_tags, "java_tags": self.java_tags}


def _tag_map(pairs, side: str, who: str, diffs: list) -> dict:
    out = {}
    for tag, arity in pairs:
        if tag in out and out[tag] != arity:
            diffs.append((who, "two-arities", f"{side} tag {tag} has arities {out[tag]} and {arity}"))
        out.setdefault(tag, arity)
    return out


def rust_tags(m: RustModule, roots) -> tuple:
    diffs = []
    pairs = []
    for r in roots:
        if r not in m.enums:
            raise VocabError(f"{m.path}: [vocab] roots names {r}, which is not an enum of msgs.rs")
        pairs += list(m.enums[r].variants.items())
    return _tag_map(pairs, "Rust", "rust", diffs), diffs


def payload_enums(m: RustModule, roots, skip: set) -> list:
    """The non-root enums reachable from the roots' variant field types, minus hand-Debug and opaque types."""
    seen, todo, out = set(roots), list(roots), []
    while todo:
        e = m.enums.get(todo.pop(0))
        if e is None:
            continue
        for t in sorted(e.field_types):
            if t in m.enums and t not in seen:
                seen.add(t)
                todo.append(t)
                if t not in skip:
                    out.append(t)
    return out


def compare(rust: RustModule | None, roots, java: dict, rust_only=(), java_only=(), opaque=(),
            values: bool = False) -> Report:
    """`java` maps a Java program id to its parsed class."""
    if rust is None or not java:
        side = "Rust" if rust is not None else "Java"
        return Report("ONE-SIDED", 0, notes=[f"one side only ({side}), nothing to compare"])
    roots = list(roots) if roots else rust_roots(rust)
    rt, diffs = rust_tags(rust, roots)
    rep = Report("IDENTICAL", 0, rust_tags=rt)
    rep.notes.append(f"Rust roots: {', '.join(roots)} ({len(rt)} tags)")
    diffs = list(diffs)
    skip = set(rust.custom_debug) | set(opaque)
    for pid, jc in java.items():
        jt = _tag_map([(t, a) for t, a, _ in jc.tags], "Java", pid, diffs)
        rep.java_tags[pid] = jt
        for tag in rt:
            if tag not in jt and tag not in rust_only:
                diffs.append((pid, "one-sided", f"tag {tag} (arity {rt[tag]}) is in Rust only"))
        for tag in jt:
            if tag not in rt and tag not in java_only:
                diffs.append((pid, "one-sided", f"tag {tag} (arity {jt[tag]}) is in Java only"))
        for tag in rt:
            if tag in jt and rt[tag] != jt[tag]:
                diffs.append((pid, "arity", f"tag {tag}: Rust arity {rt[tag]}, Java arity {jt[tag]}"))
        if values:
            for en in payload_enums(rust, roots, skip):
                rv = list(rust.enums[en].variants)
                if any(rust.enums[en].variants.values()):
                    note = f"{en}: has variants with fields; its constants are not compared"
                    if note not in rep.notes:
                        rep.notes.append(note)
                    continue
                if en not in jc.enums:
                    diffs.append((pid, "values", f"payload enum {en} has no Java enum {en}"))
                    continue
                if set(rv) != set(jc.enums[en]):  # SPEC §4: a constant encodes by its name, not its position
                    diffs.append((pid, "values", f"enum {en}: Rust {', '.join(rv)}; Java {', '.join(jc.enums[en])}"))
            skipped = sorted(s for s in skip if s in rust.enums or s in rust.custom_debug)
            note = f"values not compared for {', '.join(skipped)} (hand-written Debug or opaque)"
            if skipped and note not in rep.notes:
                rep.notes.append(note)
    rep.differences = diffs
    if diffs:
        rep.status, rep.exit = "DIFFERENT", 1
    return rep


def java_source(project: Path, cls: str) -> tuple:
    """`bench.a2a_task.A2ATask` -> (<project>/java/bench/a2a_task/A2ATask.java, ["A2ATask"]); a nested class
    `p.Outer.Inner` -> (p/Outer.java, ["Outer", "Inner"]): the file and the class path inside it."""
    parts = cls.replace("$", ".").split(".")  # a binary name `p.Outer$Inner` (Class.forName) or `p.Outer.Inner`
    base = project / "java"
    for k in range(len(parts), 0, -1):
        p = base.joinpath(*parts[:k - 1], parts[k - 1] + ".java")
        if p.is_file():
            return p, parts[k - 1:]
    raise VocabError(f"no Java source for class {cls} under {base}")


def run(cfg, values: bool = False) -> Report:
    """`daavar vocab` over a loaded configuration: every program of the project participates."""
    try:
        rust = None
        if any(p.lang == "rust" for p in cfg.programs.values()):
            msgs = cfg.root / "rust" / "src" / "msgs.rs"
            if not msgs.is_file():
                raise VocabError(f"no {msgs} (the Rust programs' vocabulary)")
            rust = parse_rust(msgs.read_text(encoding="utf-8"), str(msgs))
            if not rust.enums:
                raise VocabError(f"{msgs}: no enum")
        java = {}
        for p in cfg.programs.values():
            if p.lang != "java":
                continue
            src, inner = java_source(cfg.root, p.cls)
            java[p.id] = parse_java(src.read_text(encoding="utf-8"), str(src), inner)
        v = cfg.vocab
        return compare(rust, v.get("roots"), java, v.get("rust_only", ()), v.get("java_only", ()),
                       v.get("opaque", ()), values)
    except VocabError as e:
        return Report("UNPARSABLE", 2, notes=[str(e)])
    except (OSError, UnicodeDecodeError) as e:
        return Report("UNPARSABLE", 2, notes=[str(e)])
