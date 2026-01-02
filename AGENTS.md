Cachey is a file cache service (memory & disk).

- NO TRIVIAL COMMENTS
- Optimize for simplicity, keep code short and lean as much as possible
- Follow Rust idioms and best practices
- Latest Rust features can be used
- Descriptive variable and function names
- No wildcard imports
- Imports at top of file, never inside functions
- Explicit error handling with `Result<T, E>` over panics
- Use `eyre` when the specific error is not as important
- Place unit tests in the same file using `#[cfg(test)]` modules
- Try solving with existing dependencies before adding new ones
- Prefer well-maintained crates from crates.io
- Be mindful of allocations in hot paths
- Prefer structured logging, with "fat logs" (few logs with much parameters)
- Provide helpful error messages
- Run tests: `cargo nextest run --workspace`
- Run lint: `cargo clippy --all --all-features --tests -- -D warnings`
