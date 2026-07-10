/* Place each workspace crate's functions in its own page aligned section, sorted by name, so code changes in one
   crate cannot move another crate's functions within a cache line or page. Requires v0 symbol mangling, see build.sh.

   Patterns like _R*_8year20* match an underscore followed by a length prefixed name, which is any identifier that
   carries a disambiguator. Disambiguators are base62 numbers ending in an underscore, and in practice crate roots
   always have a disambiguator while modules and functions usually do not. For example in
   _RINvMNtCsguYIsVMFEgP_5utils5input... the utils crate root matches _5utils but its input module appears as 5input,
   without an underscore, so the patterns match crates rather than modules or functions with the same name.

   Cold (.text.unlikely.*) functions are kept at the end of their crate's section, which measured more stable than
   giving them separate sections.

   All remaining code, mostly the standard library, is left in the linker's own .text section. The .pad sections keep
   the start of the remaining code, the read-only data and the writable data at a fixed position within a page, as
   otherwise they start wherever the previous section ends and move with every size change. Absolute section
   addresses would fix everything in place without any pads. This works well under GNU ld, but rust-lld makes the
   file as large as the address range used, producing large executables. rust-lld also ignores plain `. = ALIGN(4096)`
   assignments in inserted scripts, so the pads use aligned one byte sections for compatibility with both.

   INSERT adds to the linker's built-in script instead of replacing it, keeps the executables position independent
   and works with both GNU ld and rust-lld. The crate .text sections must be placed with the rest of the .text sections
   for compatibility. rust-lld places read-only data before the code, including the .eh_frame_hdr unwinding lookup
   table. Code placed below the table gets negative offsets, which rust-lld sorts as if they were unsigned, leaving the
   table unsorted. That breaks the unwinder's binary search and makes every panic abort the process. */

SECTIONS {
  .text.year  : ALIGN(4096) {
    *(SORT_BY_NAME(.text._R*_8year20*))
    *(SORT_BY_NAME(.text.unlikely._R*_8year20*))
  }
  .text.utils : ALIGN(4096) {
    *(SORT_BY_NAME(.text._R*_5utils*))
    *(SORT_BY_NAME(.text.unlikely._R*_5utils*))
  }
  .text.bench : ALIGN(4096) {
    *(SORT_BY_NAME(.text._R*_12aoc_rs_bench*)) *(SORT_BY_NAME(.text._R*_3aoc*))
    *(SORT_BY_NAME(.text.unlikely._R*_12aoc_rs_bench*)) *(SORT_BY_NAME(.text.unlikely._R*_3aoc*))
  }
  .pad.text   : ALIGN(4096) {
    BYTE(0)
  }
} INSERT BEFORE .text;

SECTIONS {
  .pad.ro : ALIGN(4096) {
    BYTE(0)
  }
} INSERT BEFORE .rodata;

SECTIONS {
  .pad.rw : ALIGN(4096) {
    BYTE(0)
  }
} INSERT BEFORE .tdata;
