#!/usr/bin/env bash
# Run after cargo publish --workspace --dry-run --all-features.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
metadata=$(cargo metadata --manifest-path "$root/Cargo.toml" --no-deps --format-version 1)
version=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])' <<<"$metadata")
target=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])' <<<"$metadata")
packages=(core ticker trade orderbook candles funding liquidations openinterest index markprice rs)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/packages"
export CARGO_TARGET_DIR="$work/target"

for package in "${packages[@]}"; do
  tar -xzf "$target/package/tmp-registry/cryptofeed-$package-$version.crate" -C "$work/packages"
done

# Test only shipped sources. Registry patches simulate the unpublished siblings.
{
  printf '[workspace]\nresolver = "3"\nmembers = [\n'
  for package in "${packages[@]}"; do
    printf '"cryptofeed-%s-%s",\n' "$package" "$version"
  done
  printf ']\n[patch.crates-io]\n'
  for package in "${packages[@]}"; do
    printf 'cryptofeed-%s = { path = "cryptofeed-%s-%s" }\n' "$package" "$package" "$version"
  done
} > "$work/packages/Cargo.toml"
cargo test --manifest-path "$work/packages/Cargo.toml" --workspace --all-features
rm "$work/packages/Cargo.toml"

# Each consumer is a separate workspace, with no paths back to the checkout.
for mode in default minimal all; do
  mkdir -p "$work/$mode/src"
  cp "$root/scripts/package-consumer/main.rs" "$work/$mode/src/main.rs"
  {
    printf '[package]\nname = "package-consumer-%s"\nversion = "0.0.0"\nedition = "2024"\nrust-version = "1.85"\npublish = false\n' "$mode"
    printf '[dependencies]\ncryptofeed-rs = { version = "=%s"' "$version"
    if [ "$mode" != default ]; then printf ', default-features = false'; fi
    printf ' }\nserde_json = "1"\nrust_decimal = "1"\nasync-trait = "0.1"\ntokio = { version = "1", features = ["macros", "rt-multi-thread", "fs", "io-util"] }\n'
    printf '[features]\ntrade-model = ["cryptofeed-rs/trade"]\nrecording = ["cryptofeed-rs/recording"]\n'
    printf '[patch.crates-io]\n'
    for package in "${packages[@]}"; do
      printf 'cryptofeed-%s = { path = "../packages/cryptofeed-%s-%s" }\n' "$package" "$package" "$version"
    done
  } > "$work/$mode/Cargo.toml"
  case "$mode" in
    default) features=(--features trade-model) ;;
    minimal) features=(--no-default-features) ;;
    all) features=(--features trade-model,recording,cryptofeed-rs/ticker,cryptofeed-rs/orderbook,cryptofeed-rs/candles,cryptofeed-rs/funding,cryptofeed-rs/liquidations,cryptofeed-rs/openinterest,cryptofeed-rs/index,cryptofeed-rs/markprice) ;;
  esac
  cargo run --manifest-path "$work/$mode/Cargo.toml" "${features[@]}"
done
