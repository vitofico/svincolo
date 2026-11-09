# Contributing to Svincolo

Open a pull request with a focused change and the relevant test results.
Run formatting, lint and the locked test suite before requesting review.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Sign off each contribution commit with `git commit -s` to certify the
[Developer Certificate of Origin 1.1](https://developercertificate.org/).
The sign-off is a statement about your right to submit the contribution.

Report suspected vulnerabilities using the private channel in
[SECURITY.md](SECURITY.md).
