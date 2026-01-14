# FindOtlp2Records.cmake Builds otlp2records Rust library and links to extension
#
# This module finds or builds the otlp2records Rust library and creates an
# imported target for linking.
#
# Input variables: OTLP2RECORDS_SOURCE_DIR - Path to otlp2records source
# (default: ../otlp2records)
#
# Output targets: otlp2records::otlp2records - Imported static library

# Detect target triple based on platform
if(EMSCRIPTEN)
  # WebAssembly via Emscripten
  set(RUST_TARGET "wasm32-unknown-emscripten")
  set(RUST_LIB_PREFIX "lib")
  set(RUST_LIB_SUFFIX ".a")
elseif(WIN32)
  # Detect MinGW vs MSVC toolchain
  if(MINGW OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
      set(RUST_TARGET "x86_64-pc-windows-gnu")
    else()
      set(RUST_TARGET "i686-pc-windows-gnu")
    endif()
    set(RUST_LIB_PREFIX "lib")
    set(RUST_LIB_SUFFIX ".a")
  else()
    # MSVC toolchain
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
      set(RUST_TARGET "x86_64-pc-windows-msvc")
    else()
      set(RUST_TARGET "i686-pc-windows-msvc")
    endif()
    set(RUST_LIB_PREFIX "")
    set(RUST_LIB_SUFFIX ".lib")
  endif()
elseif(APPLE)
  # For macOS, CMAKE_OSX_ARCHITECTURES takes precedence for cross-compilation
  # Check for x86_64 target first (handles cross-compilation from arm64 to
  # x86_64)
  if(CMAKE_OSX_ARCHITECTURES STREQUAL "x86_64")
    set(RUST_TARGET "x86_64-apple-darwin")
  elseif(CMAKE_OSX_ARCHITECTURES STREQUAL "arm64" OR CMAKE_SYSTEM_PROCESSOR
                                                     STREQUAL "arm64")
    set(RUST_TARGET "aarch64-apple-darwin")
  else()
    # Default to x86_64 for other cases
    set(RUST_TARGET "x86_64-apple-darwin")
  endif()
  set(RUST_LIB_PREFIX "lib")
  set(RUST_LIB_SUFFIX ".a")
else()
  # Linux
  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(RUST_TARGET "aarch64-unknown-linux-gnu")
  else()
    set(RUST_TARGET "x86_64-unknown-linux-gnu")
  endif()
  set(RUST_LIB_PREFIX "lib")
  set(RUST_LIB_SUFFIX ".a")
endif()

# Build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CARGO_BUILD_TYPE "debug")
  set(CARGO_BUILD_FLAGS "")
else()
  set(CARGO_BUILD_TYPE "release")
  set(CARGO_BUILD_FLAGS "--release")
endif()

# Path to otlp2records source (defaults to submodule)
if(NOT DEFINED OTLP2RECORDS_SOURCE_DIR)
  set(OTLP2RECORDS_SOURCE_DIR
      "${CMAKE_CURRENT_SOURCE_DIR}/external/otlp2records"
      CACHE PATH "Path to otlp2records source")
endif()

# Convert to absolute path
get_filename_component(OTLP2RECORDS_SOURCE_DIR "${OTLP2RECORDS_SOURCE_DIR}"
                       ABSOLUTE)

# Verify source directory exists
if(NOT EXISTS "${OTLP2RECORDS_SOURCE_DIR}/Cargo.toml")
  message(
    FATAL_ERROR "otlp2records source not found at ${OTLP2RECORDS_SOURCE_DIR}. "
                "Set OTLP2RECORDS_SOURCE_DIR to the correct path.")
endif()

# Output library path
set(OTLP2RECORDS_LIB_NAME "${RUST_LIB_PREFIX}otlp2records${RUST_LIB_SUFFIX}")
set(OTLP2RECORDS_LIB_PATH
    "${OTLP2RECORDS_SOURCE_DIR}/target/${RUST_TARGET}/${CARGO_BUILD_TYPE}/${OTLP2RECORDS_LIB_NAME}"
)

# Also check without target triple (for simpler builds)
set(OTLP2RECORDS_LIB_PATH_SIMPLE
    "${OTLP2RECORDS_SOURCE_DIR}/target/${CARGO_BUILD_TYPE}/${OTLP2RECORDS_LIB_NAME}"
)

# Header directory
set(OTLP2RECORDS_INCLUDE_DIR "${OTLP2RECORDS_SOURCE_DIR}/include")

message(STATUS "otlp2records source: ${OTLP2RECORDS_SOURCE_DIR}")
message(STATUS "otlp2records target: ${RUST_TARGET}")
message(STATUS "otlp2records build type: ${CARGO_BUILD_TYPE}")

# Custom command to build Rust library
add_custom_command(
  OUTPUT ${OTLP2RECORDS_LIB_PATH}
  COMMAND cargo build ${CARGO_BUILD_FLAGS} --target ${RUST_TARGET} --features
          ffi
  WORKING_DIRECTORY ${OTLP2RECORDS_SOURCE_DIR}
  COMMENT "Building otlp2records Rust library (${CARGO_BUILD_TYPE})"
  VERBATIM)

# Custom target for the build
add_custom_target(
  otlp2records_build
  DEPENDS ${OTLP2RECORDS_LIB_PATH}
  COMMENT "otlp2records build target")

# Import the static library
add_library(otlp2records::otlp2records STATIC IMPORTED GLOBAL)
set_target_properties(otlp2records::otlp2records
                      PROPERTIES IMPORTED_LOCATION ${OTLP2RECORDS_LIB_PATH})
add_dependencies(otlp2records::otlp2records otlp2records_build)

# Set include directory
target_include_directories(otlp2records::otlp2records
                           INTERFACE ${OTLP2RECORDS_INCLUDE_DIR})

# Platform-specific link dependencies for Rust runtime
if(EMSCRIPTEN)
  # WASM: No additional system libraries needed
  message(STATUS "otlp2records: WASM build - no additional link libraries")
elseif(WIN32)
  # Windows: Link against system libraries Rust needs
  set_property(
    TARGET otlp2records::otlp2records
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES ws2_32 userenv bcrypt ntdll)
elseif(APPLE)
  # macOS: Link against system frameworks
  find_library(SECURITY_FRAMEWORK Security)
  find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
  set_property(
    TARGET otlp2records::otlp2records
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES ${SECURITY_FRAMEWORK}
             ${COREFOUNDATION_FRAMEWORK})
else()
  # Linux: Link against pthread, dl, and m
  set_property(
    TARGET otlp2records::otlp2records
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES pthread dl m)
endif()

# Export variables for use in parent CMakeLists.txt
set(OTLP2RECORDS_FOUND TRUE)
set(OTLP2RECORDS_INCLUDE_DIRS ${OTLP2RECORDS_INCLUDE_DIR})
set(OTLP2RECORDS_LIBRARIES otlp2records::otlp2records)

message(STATUS "otlp2records found: ${OTLP2RECORDS_SOURCE_DIR}")
message(STATUS "otlp2records library: ${OTLP2RECORDS_LIB_PATH}")
message(STATUS "otlp2records headers: ${OTLP2RECORDS_INCLUDE_DIR}")
