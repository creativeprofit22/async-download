# AGENTS.md

Async public-page downloader. Rust (`rust/`) is the primary implementation; `downloader.py` is the Python baseline.

## Commands

Rust (run from repo root):

```bash
cargo build --locked --manifest-path rust/Cargo.toml
cargo test --locked --manifest-path rust/Cargo.toml
cargo clippy --locked --all-targets --manifest-path rust/Cargo.toml -- -D warnings -W clippy::pedantic   # lint
cargo fmt --manifest-path rust/Cargo.toml   # format (CI runs with `-- --check`)
```

Python (3.11+; use `.venv/Scripts/python` on Windows):

```bash
python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -W error::ResourceWarning -m unittest -v
```

Tests use loopback servers only; no public network needed.

## Rules

- CI lives in `.github/workflows/ci.yml` and must stay green.
- Never commit with `--no-verify`.
