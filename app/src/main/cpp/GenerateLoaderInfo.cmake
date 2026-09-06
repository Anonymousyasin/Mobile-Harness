if(NOT DEFINED READELF OR NOT DEFINED LOADER OR NOT DEFINED OUTPUT)
    message(FATAL_ERROR "READELF, LOADER, and OUTPUT are required")
endif()

# Default no-op when symbols can't be located (32-bit ARM fallback).
set(start_line "")
set(workaround_line "")

execute_process(
    COMMAND "${READELF}" -s "${LOADER}"
    RESULT_VARIABLE readelf_result
    OUTPUT_VARIABLE symbols
    ERROR_QUIET
)
if(NOT readelf_result EQUAL 0)
    # Write a safe zero offset so the proot source still compiles.
    file(WRITE "${OUTPUT}" "#include <unistd.h>\nconst ssize_t offset_to_pokedata_workaround=0;\n")
    message(STATUS "Skipping loader-info generation (readelf failed; using zero offset)")
    return()
endif()

string(REPLACE "\n" ";" symbol_lines "${symbols}")
foreach(line IN LISTS symbol_lines)
    if(line MATCHES "[ \t]_start$")
        set(start_line "${line}")
    elseif(line MATCHES "[ \t]pokedata_workaround$")
        set(workaround_line "${line}")
    endif()
endforeach()

if(NOT DEFINED start_line OR NOT DEFINED workaround_line)
    # 32-bit ARM fallback: emit a zero-offset loader-info so the build continues.
    file(WRITE "${OUTPUT}" "#include <unistd.h>\nconst ssize_t offset_to_pokedata_workaround=0;\n")
    message(STATUS "Loader symbols not found (likely 32-bit ARM). Using zero-offset fallback.")
    return()
endif()

string(REGEX MATCH ":[ \t]+([0-9a-fA-F]+)" ignored "${start_line}")
set(start_hex "${CMAKE_MATCH_1}")
string(REGEX MATCH ":[ \t]+([0-9a-fA-F]+)" ignored "${workaround_line}")
set(workaround_hex "${CMAKE_MATCH_1}")
math(EXPR offset "0x${workaround_hex} - 0x${start_hex}")
file(WRITE "${OUTPUT}" "#include <unistd.h>\nconst ssize_t offset_to_pokedata_workaround=${offset};\n")
