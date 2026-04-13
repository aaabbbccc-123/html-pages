#!/usr/bin/env bash
set -euo pipefail

${BUN:-bun} build s3ZipReader.ts --target browser --format iife --outfile s3ZipReader.js --minify

# Expose S3ZipReader as a global (IIFE doesn't export by default)
python3 -c "
content = open('s3ZipReader.js').read().rstrip()
assert content.endswith('})();'), 'unexpected IIFE ending'
open('s3ZipReader.js', 'w').write(content[:-len('})();')] + 'window.S3ZipReader=S3ZipReader;})();')
"

echo "Built s3ZipReader.js ($(wc -c < s3ZipReader.js) bytes)"
