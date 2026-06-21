#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Usage:
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_SESSION_TOKEN=...
#
#   ./download_stockhouse.sh          # all files (except quotes_0.parquet)
#   ./download_stockhouse.sh all      # same thing, explicit
#   ./download_stockhouse.sh 3        # just the first 3 files (oldest)
#
# Downloads quotes_YYYY-MM-DD.parquet files from s3://pme-internal/stockhouse/
# into ~/data/stockhouse/, sorted alphabetically (== chronologically by date in
# the filename). quotes_0.parquet is always excluded.
# -----------------------------------------------------------------------------

# Expects these env vars to be exported in your shell before running:
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   AWS_SESSION_TOKEN
#   AWS_DEFAULT_REGION   (optional; defaults to us-east-1)
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is not set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is not set}"
: "${AWS_SESSION_TOKEN:?AWS_SESSION_TOKEN is not set}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Usage: ./download_stockhouse.sh [max_files]
#   max_files: number of files to download, or "all" / unset for everything
max_files="${1:-all}"

s3_bucket="pme-internal"
s3_prefix="stockhouse"
dest_dir="${HOME}/data/stockhouse"

mkdir -p "${dest_dir}"

# List parquet files in the prefix, exclude quotes_0.parquet, sort
# alphabetically (== chronologically because of YYYY-MM-DD naming),
# take the first ${max_files}.
all_files=$(aws s3 ls "s3://${s3_bucket}/${s3_prefix}/" \
    | awk '{print $4}' \
    | grep -E '\.parquet$' \
    | grep -v '^quotes_0\.parquet$' \
    | sort)

if [ "${max_files}" = "all" ]; then
    files="${all_files}"
else
    files=$(echo "${all_files}" | head -n "${max_files}")
fi

if [ -z "${files}" ]; then
    echo "No matching files found in s3://${s3_bucket}/${s3_prefix}/"
    exit 1
fi

echo "Will download ${max_files} file(s):"
echo "${files}" | sed 's/^/  /'
echo

while IFS= read -r f; do
    src="s3://${s3_bucket}/${s3_prefix}/${f}"
    dst="${dest_dir}/${f}"
    echo "Downloading ${src} -> ${dst}"
    aws s3 cp "${src}" "${dst}"
done <<< "${files}"

echo "Done"
ls -lh "${dest_dir}"