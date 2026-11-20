#!/usr/bin/env bash
# Writes a benchmark's source manifest SOURCE.md5 (witness contract §1.2.1): `# root <dir>` (daavar-relative),
# then `md5sum` lines sorted by path (byte order). Run it right after a successful build of <dir>, so the
# manifest is newer than the binary/classes and names the files compiled; a tool treats an older one as missing.
# usage: source-manifest.sh rust <crate dir> <dest>   # covers Cargo.toml, Cargo.lock, src/**/*.rs (paths relative to the crate)
#                                                       # <dest> = a target/release dir: one <bin>.SOURCE.md5 per bin of the crate;
#                                                       # or a file path: that one file
#        source-manifest.sh java <bench dir> <classes dir>   # covers java/**/*.java (not */out/*, as setup-node's javac)
#                                                       # relative to <bench dir>, and <classes dir>/**/*.class as classes/...;
#                                                       # writes <classes dir>/SOURCE.md5
set -euo pipefail
[ $# -eq 3 ] || { echo "usage: source-manifest.sh rust|java <dir> <dest>" >&2; exit 2; }
kind=$1; dir=$(realpath -e "$2"); dest=$3
: "${DAAVAR:=$(realpath "$(dirname "$0")/..")}"
root=$(realpath --relative-to="$(realpath "$DAAVAR")" "$dir")
case "$root" in ../*|/*) echo "source-manifest: $dir is outside DAAVAR=$DAAVAR" >&2; exit 2;; esac
export LC_ALL=C
# md5sum lines of the NUL-separated relative paths on stdin, run in directory $1, with prefix $2 on each path
hash_in() { (cd "$1" && sort -z | xargs -0 -r md5sum --) | sed "s|^\([0-9a-f]\{32\}  \)|\1$2|"; }
# write stdin to $1 via a temporary file in the same directory (a reader never sees a half manifest)
put() { local t; t=$(mktemp "$1.XXXXXX"); cat > "$t"; mv -f "$t" "$1"; echo "manifest: $1 ($(($(wc -l < "$1") - 1)) files)"; }
case "$kind" in
  rust)
    [ -f "$dir/Cargo.toml" ] || { echo "source-manifest: no $dir/Cargo.toml" >&2; exit 2; }
    body=$( { echo "# root $root"
              (cd "$dir" && { for f in Cargo.toml Cargo.lock; do [ -f "$f" ] && printf '%s\0' "$f"; done
                              [ -d src ] && find src -name '*.rs' -type f -print0; } ) | hash_in "$dir" ""; } )
    if [ -d "$dest" ]; then
      bins=$(cd "$dir" && cargo metadata --no-deps --format-version 1 --offline \
             | jq -r '.packages[].targets[] | select(.kind | index("bin")) | .name')
      [ -n "$bins" ] || { echo "source-manifest: no bin targets in $dir" >&2; exit 2; }
      for b in $bins; do printf '%s\n' "$body" | put "$dest/$b.SOURCE.md5"; done
    else
      printf '%s\n' "$body" | put "$dest"
    fi ;;
  java)
    if [ ! -d "$dir/java" ] || [ ! -d "$dest" ]; then echo "source-manifest: need $dir/java and the classes dir $dest" >&2; exit 2; fi
    cls=$(realpath "$dest")
    { echo "# root $root"
      { (cd "$dir" && find java -name '*.java' -type f -not -path '*/out/*' -print0) | hash_in "$dir" ""
        (cd "$cls" && find . -name '*.class' -type f -printf '%P\0') | hash_in "$cls" "classes/"
      } | sort -k2; } | put "$cls/SOURCE.md5" ;;
  *) echo "source-manifest: kind must be rust or java" >&2; exit 2 ;;
esac
