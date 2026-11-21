"""Source manifests (SOURCE.md5), byte-identical to `scripts/source-manifest.sh` (format/SPEC.md §8.2).

`# root <dir>` first: <dir> relative to the daavar checkout when the directory is inside it (as the script
writes it), else `project:<dir relative to the project root>` (DRIVER-DESIGN §3.2, O1; DRIVER-IMPL-NOTES
D6). Then one `md5sum` line per file, `<md5>  <path>`, sorted by the bytes of the path:
- rust <crate dir>: Cargo.toml, Cargo.lock, src/**/*.rs, paths relative to the crate;
- java <bench dir> <classes dir>: java/**/*.java (not under */out/*) relative to the bench dir, and the
  classes dir's **/*.class as classes/<path>.
Under src/ and java/ only regular files count (`find -type f`: no symbolic links, which find does not follow
either; `build` refuses symbolic links there, D13); Cargo.toml and Cargo.lock count as the script's `[ -f ]`.
"""

from __future__ import annotations

import fnmatch
import hashlib
import os
import tempfile
import tomllib
from pathlib import Path

from . import DaavarError


class ManifestError(DaavarError):
    pass


def daavar_checkout() -> Path:
    """The daavar checkout the driver belongs to (`DAAVAR` overrides it, as for source-manifest.sh)."""
    env = os.environ.get("DAAVAR")
    return Path(env).resolve() if env else Path(__file__).resolve().parents[1]


def driver_checkout() -> Path:
    """The checkout holding this driver (its scripts/), whatever DAAVAR says about manifest roots."""
    return Path(__file__).resolve().parents[1]


def root_of(directory: Path, project_root: Path, daavar: Path | None = None) -> str:
    d = Path(os.path.realpath(directory))
    base = Path(os.path.realpath(daavar or daavar_checkout()))
    rel = os.path.relpath(d, base)
    if rel != ".." and not rel.startswith("../") and not os.path.isabs(rel):
        return rel
    prel = os.path.relpath(d, os.path.realpath(project_root))
    if prel == ".." or prel.startswith("../"):
        raise ManifestError(f"{d} is outside both the daavar checkout {base} and the project {project_root}")
    return f"project:{prel}"


def _files(base: Path, top: str, pattern: str, exclude_out: bool = False) -> list:
    """Paths (relative to base, as bytes-sortable str) of the regular files under base/top matching pattern."""
    out = []
    start = base / top
    if start.is_symlink():  # `find src` does not follow a symbolic link given as the start either
        return out
    if not start.is_dir():
        return out
    for dirpath, dirnames, filenames in os.walk(start, followlinks=False):
        dirnames.sort()
        for f in filenames:
            p = Path(dirpath) / f
            if not fnmatch.fnmatchcase(f, pattern) or p.is_symlink() or not p.is_file():
                continue
            rel = p.relative_to(base).as_posix()
            if exclude_out and fnmatch.fnmatchcase(rel, "*/out/*"):
                continue
            out.append(rel)
    return out


def _md5(p: Path) -> str:
    h = hashlib.md5()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _line(digest: str, path: str) -> str:
    if "\\" in path or "\n" in path or "\r" in path:
        raise ManifestError(f"{path!r}: md5sum would escape this name; rename the file")
    return f"{digest}  {path}"


def _sorted(paths) -> list:
    return sorted(paths, key=lambda s: os.fsencode(s))


def rust(crate: Path, project_root: Path, daavar: Path | None = None) -> str:
    crate = Path(crate)
    if not (crate / "Cargo.toml").is_file():
        raise ManifestError(f"no {crate}/Cargo.toml")
    # `[ -f ]` in the script: Cargo.toml and Cargo.lock count even as symbolic links to files.
    paths = [f for f in ("Cargo.toml", "Cargo.lock") if (crate / f).is_file()]
    paths += _files(crate, "src", "*.rs")
    lines = [f"# root {root_of(crate, project_root, daavar)}"]
    lines += [_line(_md5(crate / p), p) for p in _sorted(paths)]
    return "\n".join(lines) + "\n"


def java(bench: Path, classes: Path | None, project_root: Path, daavar: Path | None = None) -> str:
    """The Java manifest; with classes None, the source part only (the build key's input)."""
    bench = Path(bench)
    if not (bench / "java").is_dir():
        raise ManifestError(f"no {bench}/java")
    entries = [(p, _line(_md5(bench / p), p)) for p in _files(bench, "java", "*.java", exclude_out=True)]
    if classes is not None:
        cls = Path(os.path.realpath(classes))
        if not cls.is_dir():
            raise ManifestError(f"no classes directory {classes}")
        for rel in _files(cls, ".", "*.class"):
            rel = rel[2:] if rel.startswith("./") else rel
            entries.append(("classes/" + rel, _line(_md5(cls / rel), "classes/" + rel)))
    lines = [f"# root {root_of(bench, project_root, daavar)}"]
    lines += [l for _, l in sorted(entries, key=lambda e: os.fsencode(e[0]))]
    return "\n".join(lines) + "\n"


def files_of(text: str) -> dict:
    """path -> md5 of a manifest's lines."""
    out = {}
    for l in text.splitlines()[1:]:
        d, p = l.split("  ", 1)
        out[p] = d
    return out


def write(text: str, dest: Path) -> None:
    """Via a temporary file in the same directory, so a reader never sees half a manifest."""
    dest = Path(dest)
    fd, tmp = tempfile.mkstemp(prefix=dest.name + ".", dir=dest.parent)
    with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.chmod(tmp, 0o644)
    os.replace(tmp, dest)


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def rust_bins(crate: Path) -> list:
    """The crate's binary targets, as `cargo metadata` lists them: [[bin]] names, plus the package's own
    bin for src/main.rs and src/bin/*.rs unless `autobins = false`."""
    try:
        with open(Path(crate) / "Cargo.toml", "rb") as f:
            t = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ManifestError(f"{crate}/Cargo.toml: {e}") from None
    pkg = t.get("package", {})
    name = pkg.get("name")
    if not name:
        raise ManifestError(f"{crate}/Cargo.toml: no [package] name")
    bins, paths = [], set()
    for b in t.get("bin", []):
        bins.append(b["name"])
        paths.add(b.get("path", f"src/bin/{b['name']}.rs"))
    if pkg.get("autobins", True):
        if (Path(crate) / "src/main.rs").is_file() and "src/main.rs" not in paths and name not in bins:
            bins.append(name)
        bd = Path(crate) / "src/bin"
        if bd.is_dir():
            for p in sorted(bd.iterdir()):
                if p.is_file() and p.suffix == ".rs":
                    if f"src/bin/{p.name}" not in paths and p.stem not in bins:
                        bins.append(p.stem)
                elif p.is_dir() and (p / "main.rs").is_file():  # src/bin/<name>/main.rs
                    if f"src/bin/{p.name}/main.rs" not in paths and p.name not in bins:
                        bins.append(p.name)
    return bins
