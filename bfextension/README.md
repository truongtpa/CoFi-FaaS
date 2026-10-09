```
cargo install cargo-duckdb-ext-tools
cargo duckdb-ext-build
cargo duckdb-ext-build -- --release
cargo build 2>&1
```

```
rustup target add x86_64-unknown-linux-gnu
cargo install cargo-zigbuild
brew install zig
cargo zigbuild --release --target x86_64-unknown-linux-gnu
cargo build --release
``leáe