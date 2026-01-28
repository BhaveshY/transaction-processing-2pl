# cmake/common/CompilerWarnings.cmake

function(set_project_warnings target)
    set(MSVC_WARNINGS
            /W4
            /permissive-
            /w14242
            /w14254
            /w14263
            /w14265
            /w14287
            /we4289
            /w14296
            /w14311
            /w14545
            /w14546
            /w14547
            /w14549
            /w14555
            /w14640
            /w14826
            /w14905
            /w14906
            /w14928
    )

    set(GCC_CLANG_WARNINGS
            -Wall
            -Wextra
            -Wpedantic
            -Wcast-align
            -Wformat=2
            -Wduplicated-cond
            -Wlogical-op
            -Wnull-dereference
            -Wdouble-promotion
    )

    if (MSVC)
        target_compile_options(${target} PRIVATE ${MSVC_WARNINGS})
    else()
        target_compile_options(${target} PRIVATE ${GCC_CLANG_WARNINGS})
    endif()
endfunction()