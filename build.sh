#!/usr/bin/env bash
# ==============================================================================
# NexusKV Multi-Language Build Script
# Builds Go Control Plane, Rust Core Engine & PyO3 Native Bindings
# ==============================================================================

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${REPO_ROOT}/bin"

mkdir -p "${BIN_DIR}"

echo "======================================================================"
echo "                   NexusKV Unified Build Pipeline"
echo "======================================================================"

# 1. Generate Schema & Contract Bindings
echo "==> [1/3] Generating Contract Bindings..."
python3 "${REPO_ROOT}/tools/generate_contracts.py"

# 2. Build Go Control Plane Binary
echo "==> [2/3] Building Go Control Plane (nexuskv-controlplane)..."
cd "${REPO_ROOT}/go"
GOTOOLCHAIN=go1.25.9 go build -o "${BIN_DIR}/nexuskv-controlplane" ./cmd/nexuskv-controlplane
echo "    ✔ Built Go binary: ${BIN_DIR}/nexuskv-controlplane"

# 3. Build Rust Core Engine & PyO3 Native Bindings
echo "==> [3/3] Building Rust Core Engine & PyO3 Native Bindings..."
cd "${REPO_ROOT}/rust"

# On macOS, PyO3 extension modules require dynamic lookup for Python C-API symbols
if [ "$(uname -s)" = "Darwin" ]; then
    export RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup ${RUSTFLAGS:-}"
fi

cargo build --release -p bindings-py

# Determine OS dynamic library extension
OS_NAME="$(uname -s)"
case "${OS_NAME}" in
    Linux*)     DYLIB_EXT="so";;
    Darwin*)    DYLIB_EXT="dylib";;
    CYGWIN*|MINGW*|MSYS*) DYLIB_EXT="dll";;
    *)          DYLIB_EXT="so";;
esac

TARGET_DYLIB="${REPO_ROOT}/rust/target/release/libnexuskv_planner_native.${DYLIB_EXT}"
PYTHON_NATIVE_DIR="${REPO_ROOT}/python/nexuskv/planner"

if [ -f "${TARGET_DYLIB}" ]; then
    cp "${TARGET_DYLIB}" "${PYTHON_NATIVE_DIR}/nexuskv_planner_native.${DYLIB_EXT}"
    echo "    ✔ Copied PyO3 native module to: ${PYTHON_NATIVE_DIR}/nexuskv_planner_native.${DYLIB_EXT}"
fi

# 4. Build Python Wheel (.whl) package using Maturin (if installed)
if command -v maturin &> /dev/null; then
    echo "==> [4/4] Building Python Wheel (.whl) package with Maturin..."
    maturin build --release --manifest-path "${REPO_ROOT}/rust/crates/bindings-py/Cargo.toml" --out "${REPO_ROOT}/dist"
    echo "    ✔ Built Python wheel package in: ${REPO_ROOT}/dist/"
else
    echo "==> [Note] 'maturin' CLI not installed. To build .whl packages, run: pip install maturin && ./build.sh"
fi

echo "======================================================================"
echo "             NexusKV Build Complete! All binaries ready."
echo "======================================================================"
