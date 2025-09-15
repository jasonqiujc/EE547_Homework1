#!/bin/bash
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <input_file> <output_directory>"
  exit 1
fi

INPUT_FILE="$1"
OUTPUT_DIR="$2"

if [ ! -f "$INPUT_FILE" ]; then
  echo "Error: Input file $INPUT_FILE does not exist"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

docker run --rm \
  --name http-fetcher \
  -v "$(realpath "$INPUT_FILE")":/data/input/urls.txt:ro \
  -v "$(realpath "$OUTPUT_DIR")":/data/output \
  http-fetcher:latest
