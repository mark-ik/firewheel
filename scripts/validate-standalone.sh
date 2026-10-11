#!/usr/bin/env bash
# Local, device-free qualification of the maintained standalone stream contract.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
toolchain="${FIREWHEEL_VALIDATION_TOOLCHAIN:-1.97.1}"
validation_dir="$(mktemp -d "${TMPDIR:-/tmp}/firewheel-validation.XXXXXX")"
trap 'rm -rf "$validation_dir"' EXIT
cd "$validation_dir"

# Libraries do not publish a lockfile. Generate a local one only when absent;
# subsequent checks use that same dependency resolution without updating it.
if [[ ! -f "$root/Cargo.lock" ]]; then
  cargo "+$toolchain" generate-lockfile --manifest-path "$root/Cargo.toml"
fi

cargo "+$toolchain" test --manifest-path "$root/Cargo.toml" --locked \
  -p firewheel --no-default-features --features std,stream_nodes --test stream_nodes
cargo "+$toolchain" test --manifest-path "$root/Cargo.toml" --locked \
  -p firewheel-nodes --no-default-features --features stream --lib
cargo "+$toolchain" check --manifest-path "$root/Cargo.toml" --locked \
  -p firewheel --no-default-features --features libm
cargo "+$toolchain" tree --manifest-path "$root/Cargo.toml" --locked \
  -p firewheel --no-default-features --features std,stream_nodes \
  --edges normal,build --prefix none > "$validation_dir/dependencies.txt"
if grep -q '^bevy_ecs ' "$validation_dir/dependencies.txt"; then
  cat "$validation_dir/dependencies.txt"
  echo "standalone streams unexpectedly enable Bevy ECS" >&2
  exit 1
fi
cargo "+$toolchain" fmt --manifest-path "$root/Cargo.toml" --all -- --check
