FROM rust:1.97.1-alpine@sha256:3c38f3f82c2f3d73da3b38e18d279393a04cb43ddded0e35088a8c3324d40900 AS build
RUN apk add --no-cache build-base
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install --locked --version 0.7.7 cargo-auditable
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    target="$(rustc --print host-tuple)" \
    && RUSTFLAGS="-C target-feature=+crt-static" cargo auditable build --release --locked --target "$target" \
    && cp "target/$target/release/svincolo" /svincolo

FROM scratch
COPY --from=build /svincolo /svincolo
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/svincolo"]
CMD ["serve"]
