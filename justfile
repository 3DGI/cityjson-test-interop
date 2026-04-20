set shell := ["bash", "-lc"]

# Show available recipes.
_default:
    just --list

# Install Python dependencies.
sync:
    uv sync

# Run the black-box interoperability suite.
test:
    uv run pytest

# Compile the Rust helper that exercises public crate APIs.
check-rust:
    cargo check --manifest-path rust-tools/Cargo.toml

# Run all checks.
ci: check-rust test
