
function(enable_sanitizers target)
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")

        option(ENABLE_ASAN  "Enable AddressSanitizer"      ON)
        option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" ON)
        option(ENABLE_TSAN  "Enable ThreadSanitizer"       OFF)

        # Cannot combine address sanitizer and thread sanitizer
        if (ENABLE_ASAN AND ENABLE_TSAN)
            message(FATAL_ERROR "ASan and TSan cannot be used together.")
        endif()

        # TSan conflicts with UBSan (runtime incompatibility)
        if (ENABLE_UBSAN AND ENABLE_TSAN)
            message(FATAL_ERROR "TSan and UBSan cannot be used together.")
        endif()

        # -----------------------------
        # AddressSanitizer
        # -----------------------------
        if (ENABLE_ASAN)
            target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
            target_link_options(${target} PRIVATE -fsanitize=address)
        endif()

        # -----------------------------
        # UndefinedBehaviorSanitizer
        # -----------------------------
        if (ENABLE_UBSAN)
            target_compile_options(${target} PRIVATE -fsanitize=undefined)
            target_link_options(${target} PRIVATE -fsanitize=undefined)
        endif()

        # -----------------------------
        # ThreadSanitizer
        # -----------------------------
        if (ENABLE_TSAN)
            if (APPLE)
                # macOS has good TSan support, but requires linking libc++
                target_compile_options(${target} PRIVATE -fsanitize=thread)
                target_link_options(${target} PRIVATE -fsanitize=thread)
            else()
                # Linux (Clang or GCC)
                target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
                target_link_options(${target} PRIVATE -fsanitize=thread)
            endif()
        endif()

    elseif (MSVC)
        if (ENABLE_TSAN OR ENABLE_ASAN OR ENABLE_UBSAN)
            message(WARNING "Sanitizers are not supported on MSVC.")
        endif()
    endif()
endfunction()