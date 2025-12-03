# ParquetViewer

ParquetViewer is a Rust application that uses Apache Arrow and Parquet to load columnar data and presents it in a lightweight GPUI window. The tool also supports a headless mode for quick terminal previews.

## Features
- Read Parquet metadata and schema using the `parquet` and `arrow` crates
- Preview the first N rows as a formatted table
- GPUI window to browse schema details and row samples
- Headless mode for terminal output

## Getting Started
### Prerequisites
- Rust 1.70+ with Cargo
- Network access to download crate dependencies (Arrow, Parquet, GPUI)

### Build
From the repository root:

```bash
cargo build
```

### Usage

```bash
# Launch the GPUI viewer (default)
cargo run -- path/to/file.parquet --rows 25

# Print the preview to stdout without the UI
cargo run -- path/to/file.parquet --rows 25 --headless
```

### Project Layout
- `src/main.rs`: CLI entry point, Parquet loading, and GPUI renderer
- `Cargo.toml`: Rust package metadata and dependencies

### Development
Format the code with Rustfmt:

```bash
cargo fmt
```

Run clippy and the build (requires access to crates.io for dependencies):

```bash
cargo clippy
cargo test
```

### License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
