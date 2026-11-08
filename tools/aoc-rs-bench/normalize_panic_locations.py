#!/usr/bin/env python3
"""Normalize the aoc-rs panic locations in a binary, so its bytes no longer depend on where the panic sites are.

rustc embeds a core::panic::Location for each panic site: a pointer to the file name, the name's length, then the line
and column as u32s. An edit moves every panic site below it, so adding a helper to a shared file changes every binary
that can panic in that file, even though no code changed. These fields are normally only read when a panic is reported,
so normalizing them lets such binaries dedupe, provided the benchmarked code doesn't read them some other way, such as
through Location::caller().

Each line and column is replaced with the tag b"aocbench", which a panic prints as 1650683745:1751346789, so a
normalized location is easy to recognise and search for. The file name is kept.

Locations are recognised heuristically. A record is treated as an aoc-rs Location when:
- An R_X86_64_RELATIVE relocation at an 8-aligned address points at a printable "/aoc-rs/crates/...rs" name, followed
  by the name's length.
- From rustc 1.89 the name is NUL-terminated, which rustc does for Location names. 1.89 counts the NUL in the length.
- The line is within 1 to MAX_LINE and the column within 1 to MAX_COLUMN.
- The position is inside the named file in the source tree, on an identifier character, `[` or `(`. Calls and macros
  point at their first character, indexing and slicing at the bracket.

Records pointing at /rustc/ names get the same checks apart from the source position, and there must be at least
MIN_STD_LOCATIONS of them. A failed check, or a crates/aoc/, crates/utils/ or crates/yearNNNN/ path outside /aoc-rs/,
is an error, so a change to how Locations are stored, or to the remap in build.sh, fails the build.

Only x86-64 ELF executables are supported.

Usage: normalize_panic_locations.py <binary> <source-tree>
"""
import re
import struct
import sys
from pathlib import Path

TAG = b"aocbench"  # Printed as line 1650683745, column 1751346789
PREFIX = b"/aoc-rs/"
CRATES = PREFIX + b"crates/"
STD_PREFIX = b"/rustc/"
# Every binary links std's panic machinery, which has 34 to 176 Locations depending on the toolchain
MIN_STD_LOCATIONS = 20
# Real Locations in the store reach line 3950 and column 89, which leaves room for longer files and lines
MAX_LINE = 10_000
MAX_COLUMN = 200
MAX_NAME_LENGTH = 4096
R_X86_64_RELATIVE = 8
PANIC_SITE = re.compile(r"[A-Za-z_\[(]")
# An aoc-rs crate file under any prefix, allowing the NUL that 1.89 counts in the length
AOC_RS_CRATE = re.compile(rb"(?:^|/)crates/(?:aoc|utils|year\d{4})/[ -~]*\.rs\0?\Z")


class Elf:
    """The parts of an x86-64 ELF executable needed to find the pointers it stores."""

    def __init__(self, data: bytes):
        if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1 or struct.unpack_from("<H", data, 18)[0] != 62:
            raise RuntimeError("not a little-endian x86-64 ELF64 file")
        self.data = data
        phoff, = struct.unpack_from("<Q", data, 0x20)
        phentsize, phnum = struct.unpack_from("<HH", data, 0x36)
        self.segments = []
        for i in range(phnum):
            p_type, _, p_offset, p_vaddr, _, p_filesz = struct.unpack_from("<IIQQQQ", data, phoff + i * phentsize)
            if p_type == 1:  # PT_LOAD
                self.segments.append((p_vaddr, p_offset, p_filesz))
        shoff, = struct.unpack_from("<Q", data, 0x28)
        shentsize, shnum, shstrndx = struct.unpack_from("<HHH", data, 0x3a)
        headers = [struct.unpack_from("<IIQQQQ", data, shoff + i * shentsize) for i in range(shnum)]
        names = headers[shstrndx][4]
        self.sections = {}
        for sh_name, _, _, _, sh_offset, sh_size in headers:
            end = data.index(b"\0", names + sh_name)
            self.sections[data[names + sh_name:end].decode()] = (sh_offset, sh_size)

    def offset(self, address: int):
        """The file offset of a virtual address, or None when no loaded segment has file contents there."""
        for p_vaddr, p_offset, p_filesz in self.segments:
            if p_vaddr <= address < p_vaddr + p_filesz:
                return address - p_vaddr + p_offset
        return None

    def relative_relocations(self):
        """The address and target of each R_X86_64_RELATIVE relocation, which is how a PIE stores a pointer."""
        if ".rela.dyn" not in self.sections:
            raise RuntimeError("no .rela.dyn section")
        start, size = self.sections[".rela.dyn"]
        for i in range(start, start + size, 24):
            r_offset, r_info, r_addend = struct.unpack_from("<QQq", self.data, i)
            if r_info & 0xffffffff == R_X86_64_RELATIVE:
                yield r_offset, r_addend

    def rustc_version(self):
        """The (major, minor) rustc version recorded in .comment."""
        start, size = self.sections.get(".comment", (0, 0))
        m = re.search(rb"rustc version (\d+)\.(\d+)", self.data[start:start + size])
        if not m:
            raise RuntimeError("no rustc version in .comment")
        return int(m.group(1)), int(m.group(2))


def source_lines(tree: Path, name: str, cache: dict):
    """The lines of a file in the source tree, or None when it does not exist."""
    if name not in cache:
        path = tree / name
        cache[name] = path.read_text().split("\n") if path.is_file() else None
    return cache[name]


def normalize_panic_locations(binary: Path, tree: Path) -> int:
    """Normalize every aoc-rs Location in the binary in place, returning how many there are."""
    elf = Elf(binary.read_bytes())
    data = elf.data
    nul_terminated = elf.rustc_version() >= (1, 89)
    sources = {}
    fields = []
    std_locations = 0
    for address, target in elf.relative_relocations():
        field, name_start = elf.offset(address), elf.offset(target)
        if field is None or name_start is None or field + 24 > len(data):
            continue
        length, = struct.unpack_from("<Q", data, field + 8)
        # An empty &str can point anywhere, including at the start of a file name
        if length == 0:
            continue
        is_std = data.startswith(STD_PREFIX, name_start)
        if not is_std and not data.startswith(PREFIX, name_start):
            if length <= MAX_NAME_LENGTH and AOC_RS_CRATE.search(data[name_start:name_start + length]):
                raise RuntimeError(f"pointer at {address:#x} to {data[name_start:name_start + length]!r}: an aoc-rs "
                                   f"crate path outside /aoc-rs/, has the remap in build.sh changed?")
            continue

        where = f"pointer at {address:#x} to {data[name_start:name_start + 80]!r}"
        if length > MAX_NAME_LENGTH or name_start + length >= len(data):
            raise RuntimeError(f"{where}: followed by {length}, which is not the name's length")
        stored = data[name_start:name_start + length]
        name = stored[:-1] if stored.endswith(b"\0") else stored
        if address % 8 or not name.endswith(b".rs") or not all(32 <= c < 127 for c in name):
            raise RuntimeError(f"{where}: not shaped like a Location")
        if nul_terminated and not stored.endswith(b"\0") and data[name_start + length] != 0:
            raise RuntimeError(f"{where}: name is not NUL-terminated like a Location's")
        tagged = data[field + 16:field + 24] == TAG
        line, column = struct.unpack_from("<II", data, field + 16)
        if not tagged and (not 1 <= line <= MAX_LINE or not 1 <= column <= MAX_COLUMN):
            raise RuntimeError(f"{where}: line {line} and column {column} are outside 1-{MAX_LINE} and 1-{MAX_COLUMN}")

        if is_std:
            std_locations += 1
            continue
        if not name.startswith(CRATES):
            raise RuntimeError(f"{where}: not under /aoc-rs/crates/")
        if not tagged:
            rel = name[len(PREFIX):].decode()
            lines = source_lines(tree, rel, sources)
            if lines is None:
                raise RuntimeError(f"{where}: {rel} is not in {tree}")
            # The range check means the line and column are at least 1, and match returns None past the line's end
            text = lines[line - 1] if line <= len(lines) else ""
            if not PANIC_SITE.match(text, column - 1):
                raise RuntimeError(f"{where}: {rel}:{line}:{column} is outside the file or not on an identifier, "
                                   f"`[` or `(` in {tree}")
        fields.append(field)

    if std_locations < MIN_STD_LOCATIONS:
        raise RuntimeError(f"found {std_locations} std Locations, expected at least {MIN_STD_LOCATIONS}")
    normalized = bytearray(data)
    for field in fields:
        normalized[field + 16:field + 24] = TAG
    binary.write_bytes(normalized)
    return len(fields)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__.strip().splitlines()[-1])
    binary, tree = Path(sys.argv[1]), Path(sys.argv[2])
    try:
        normalize_panic_locations(binary, tree)
    except RuntimeError as e:
        sys.exit(f"normalize_panic_locations.py: {binary}: {e}")


if __name__ == "__main__":
    main()
