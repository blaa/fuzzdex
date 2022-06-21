#!/bin/bash

echo "Building version"
egrep '^version' Cargo.toml

rm -rf target/wheels/*whl

# manylinux build using docker.
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release -i python3.10
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release -i python3.9
docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release -i python3.7

# Manual upload:
# twine upload target/wheels/fuzzdex-*-cp*-cp*-manylinux_2_5_x86_64.manylinux1_x86_64.whl 
