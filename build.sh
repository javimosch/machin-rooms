#!/usr/bin/env bash
# Build machin-rooms into one native binary. Needs machin v0.79.0+ (WebSocket server) and
# a C compiler — no zig, no Node.
set -euo pipefail
cd "$(dirname "$0")"
MACHIN="${MACHIN:-machin}"
"$MACHIN" encode src/machweb.src src/ws.src src/rooms.src > rooms.mfl
"$MACHIN" build rooms.mfl -o machin-rooms
echo "built ./machin-rooms"
