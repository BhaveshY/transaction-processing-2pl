function(enable_clang_tidy target)
    find_program(CLANG_TIDY_EXE NAMES clang-tidy)

    if (CLANG_TIDY_EXE MATCHES "CLANG_TIDY_EXE-NOTFOUND")
        message(WARNING " clang-tidy not found!")
        return()
    endif()

    set(DO_CLANG_TIDY
            "${CLANG_TIDY_EXE}"
            --warnings-as-errors=*
            --extra-arg=-std=c++20
    )

    set_target_properties(${target} PROPERTIES
            CXX_CLANG_TIDY "${DO_CLANG_TIDY}"
    )
endfunction()