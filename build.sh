#!/usr/bin/env bash
set -euo pipefail

${BUN:-bun} build s3ZipReader.ts --target browser --format esm --outfile s3ZipReader_esm.js --minify
