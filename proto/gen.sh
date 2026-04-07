#!/usr/bin/env bash
# regen stubs into ../pb
set -euo pipefail
cd "$(dirname "$0")/.."

protoc \
  --go_out=pb --go_opt=paths=source_relative \
  --go-grpc_out=pb --go-grpc_opt=paths=source_relative \
  -I proto \
  raft.proto
