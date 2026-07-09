/* Place each crate's functions in its own page aligned section, sorted by name, so code changes in one crate cannot
   move another crate's functions within a cache line or page. Requires v0 symbol mangling, see build.sh.

   Patterns like _R*_8year20* match an underscore followed by a length prefixed name, which is any identifier that
   carries a disambiguator. Disambiguators are base62 numbers ending in an underscore, and in practice crate roots
   always have a disambiguator while modules and functions usually do not. For example in
   _RINvMNtCsguYIsVMFEgP_5utils5input... the utils crate root matches _5utils but its input module appears as 5input,
   without an underscore, so the patterns match crates rather than modules or functions with the same name.

   Cold (.text.unlikely.*) functions are kept at the end of their crate's section, which measured more stable than
   giving them separate sections.

   The .pad sections keep the start of the read-only data and the writable data at a fixed position within a page, as
   otherwise they start wherever the previous section ends and move with every code size change. Absolute section
   addresses would fix everything in place without any pads. This works well under GNU ld, but rust-lld makes the file
   as large as the address range used, producing large executables. Relative sections with the pads keep the file small
   under both linkers.

   INSERT BEFORE adds to the linker's built-in script instead of replacing it, keeps the executables position
   independent and works with both GNU ld and rust-lld. */

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
  .text.misc  : ALIGN(4096) {
    *(SORT_BY_NAME(.text.*))
  }
  .pad.ro     : ALIGN(4096) {
    BYTE(0)
  }
} INSERT BEFORE .rodata;

SECTIONS {
  .pad.rw : ALIGN(4096) {
    BYTE(0)
  }
} INSERT BEFORE .tdata;
