#!/usr/bin/env python3
"""Check an installed tool version against a floor. Stdlib-only, portable (no `sort -V`).

Usage:  python3 -I version_floor.py "<raw tool --version output>" <floor>
        python3 -I version_floor.py --self-test

The first argument may be the raw, multi-line output of a `--version` command; the
version is the first dotted number following the word "version" on the same line (within
~20 chars, e.g. "tool version is 4.0.0"), else the first dotted number anywhere (e.g. "kafka-topics 3.6.1 (Commit: …)"). Comparison is numeric per
component with missing components treated as 0 (4.0 == 4.0.0); pre-release suffixes
(4.0.0-preview1) compare as the release.

Exit status: 0 have >= floor, 1 have < floor, 2 no version found / bad floor / usage.
"""

from __future__ import annotations

import re
import sys

DOTTED = r"\d+(?:\.\d+)+"
# "version" followed (on the same line, within a short phrase) by the dotted number.
AFTER_WORD_RE = re.compile(r"\bversion\b[^\d\r\n]{0,20}(" + DOTTED + r")", re.I)
ANY_RE = re.compile(r"(?<![\w.])(" + DOTTED + r")")
FLOOR_RE = re.compile(r"\d+(?:\.\d+)*")


def extract(text: str):
    m = AFTER_WORD_RE.search(text) or ANY_RE.search(text)
    return m.group(1) if m else None


def key(version: str, width: int) -> tuple[int, ...]:
    parts = [int(p) for p in version.split(".")]
    return tuple(parts + [0] * (width - len(parts)))


def at_least(have: str, need: str) -> bool:
    width = max(have.count("."), need.count(".")) + 1
    return key(have, width) >= key(need, width)


def self_test() -> int:
    cases = [
        ("Welcome to ... version 4.0.1\nUsing Scala version 2.13.16", "4.0", 0),
        ("kafka-topics 3.6.1 (Commit:5e3c2b738d253ff5)", "3.7", 1),
        ("3.0.2", "3.0.2", 0),
        ("mlflow, version 2.21.0", "2.9", 0),   # numeric, not lexical: 21 > 9
        ("4.0", "4.0.0", 0),                     # missing components are 0
        ("4.0.0-preview1", "4.0", 0),
        ("Built 2026.10.21; tool version is 4.0.0", "4.0", 0),   # not the build date
        ("Built 2026.10.21; tool version is 3.9.0", "4.0", 1),
        ("command not found", "1.0", 2),
    ]
    failures = []
    for text, floor, want in cases:
        have = extract(text)
        got = 2 if have is None else (0 if at_least(have, floor) else 1)
        print(f"  {'ok  ' if got == want else 'FAIL'} {text.splitlines()[0][:40]!r} vs {floor} -> {got}")
        if got != want:
            failures.append(text)
    print("self-test OK" if not failures else f"self-test FAILED ({len(failures)})")
    return 1 if failures else 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) != 2:
        print(__doc__.split("\n\n", 2)[1], file=sys.stderr)
        return 2
    text, floor = argv
    if not FLOOR_RE.fullmatch(floor.strip()):
        print(f"version_floor: floor {floor!r} must be a dotted number like 4.0 or 3.6.1", file=sys.stderr)
        return 2
    have = extract(text)
    if have is None:
        print("version_floor: no version number found in the tool output (is it installed?)",
              file=sys.stderr)
        return 2
    if at_least(have, floor.strip()):
        print(f"version_floor: OK {have} >= {floor}")
        return 0
    print(f"version_floor: TOO OLD {have} < {floor} — upgrade before continuing")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
