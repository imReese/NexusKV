#!/usr/bin/env bash
# ==============================================================================
# NexusKV Multi-Language Build Pipeline
# Builds Go Binaries (Control Plane & Server), Rust Core & PyO3 Native Bindings,
# and Maturin Python Wheel Packages (.whl)
# ==============================================================================

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${REPO_ROOT}/bin"
DIST_DIR="${REPO_ROOT}/dist"

mkdir -p "${BIN_DIR}"
mkdir -p "${DIST_DIR}"

echo "======================================================================"
echo "                   NexusKV Unified Build Pipeline"
echo "======================================================================"

# 1. Generate Schema & Contract Bindings
echo "==> [1/4] Generating Contract Bindings..."
python3 "${REPO_ROOT}/tools/generate_contracts.py"

# 2. Build Go Binaries
echo "==> [2/4] Building Go Control Plane & Server Binaries..."
cd "${REPO_ROOT}/go"
GOTOOLCHAIN=go1.25.9 go build -o "${BIN_DIR}/nexuskv-controlplane" ./cmd/nexuskv-controlplane
cd "${REPO_ROOT}"
GOTOOLCHAIN=go1.25.9 go build -o "${BIN_DIR}/nexuskv-server" ./cmd/server
echo "    ✔ Built Go controlplane binary : ${BIN_DIR}/nexuskv-controlplane"
echo "    ✔ Built Go server binary       : ${BIN_DIR}/nexuskv-server"

# 3. Build Rust Core Engine & PyO3 Native Bindings
echo "==> [3/4] Building Rust Core Engine & PyO3 Native Bindings..."
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
    echo "    ✔ Copied PyO3 native module to : ${PYTHON_NATIVE_DIR}/nexuskv_planner_native.${DYLIB_EXT}"
fi

# 4. Build Python Wheel (.whl) package using Maturin (if installed)
if command -v maturin &> /dev/null; then
    echo "==> [4/4] Building Python Wheel (.whl) Package with Maturin..."
    maturin build --release --manifest-path "${REPO_ROOT}/rust/crates/bindings-py/Cargo.toml" --out "${DIST_DIR}"
    echo "    ✔ Built Python wheel package in: ${DIST_DIR}/"
else
    echo "==> [Note] 'maturin' CLI not installed. To build .whl packages, run: pip install maturin && ./build.sh"
fi

echo "======================================================================"
echo "             NexusKV Build Complete! Artifacts Overview:"
echo "• Go Binaries     : ${BIN_DIR}/"
echo "• Python Wheel    : ${DIST_DIR}/"
echo "• Rust PyO3 Native: ${PYTHON_NATIVE_DIR}/nexuskv_planner_native.${DYLIB_EXT}"
echo "======================================================================"
