#!/usr/bin/env bash
set -euo pipefail

# Download and decompress the ClickBench hits TSV dataset for ingest_chunks.py.
# Usage:
#   ./download_hits_tsv.sh
#   ./download_hits_tsv.sh --dir /data/clickbench
#   ./download_hits_tsv.sh --keep-gz
#   ./download_hits_tsv.sh --force

DATASET_URL="https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz"
OUT_DIR="."
KEEP_GZ=0
FORCE=0

usage() {
  cat <<USAGE
Usage: $0 [options]

Options:
  --dir DIR      Directory to download into. Default: current directory
  --keep-gz      Keep hits.tsv.gz after decompression
  --force        Re-download and re-decompress even if output files exist
  -h, --help     Show this help

Produces:
  DIR/hits.tsv

Then run ingest_chunks.py with:
  python3 ingest_chunks.py --file DIR/hits.tsv ...
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --keep-gz)
      KEEP_GZ=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

GZ="hits.tsv.gz"
TSV="hits.tsv"

have_cmd() { command -v "$1" >/dev/null 2>&1; }

if ! have_cmd curl && ! have_cmd wget; then
  echo "ERROR: need curl or wget installed" >&2
  exit 1
fi

if ! have_cmd gzip; then
  echo "ERROR: need gzip installed" >&2
  exit 1
fi

if [[ -f "$TSV" && "$FORCE" -eq 0 ]]; then
  echo "Already exists: $(pwd)/$TSV"
  echo "Use --force to download/decompress again."
  exit 0
fi

if [[ "$FORCE" -eq 1 ]]; then
  rm -f "$TSV"
fi

if [[ ! -f "$GZ" || "$FORCE" -eq 1 ]]; then
  echo "Downloading $DATASET_URL"
  echo "Output: $(pwd)/$GZ"

  # Resume partial downloads where possible.
  if have_cmd curl; then
    curl -L --fail --continue-at - --output "$GZ" "$DATASET_URL"
  else
    wget --continue --output-document="$GZ" "$DATASET_URL"
  fi
else
  echo "Already exists: $(pwd)/$GZ"
fi

echo "Validating gzip stream..."
gzip -t "$GZ"

# Rough free-space check. The TSV is large, so fail early when there is clearly not enough room.
# macOS and Linux both support df -Pk.
FREE_KB=$(df -Pk . | awk 'NR==2 {print $4}')
GZ_KB=$(du -k "$GZ" | awk '{print $1}')
# Conservative estimate: uncompressed ClickBench TSV is several times larger than gzip.
NEEDED_KB=$((GZ_KB * 6))

if [[ "$FREE_KB" -lt "$NEEDED_KB" ]]; then
  echo "WARNING: available disk space may be too low." >&2
  echo "Free:      $((FREE_KB / 1024 / 1024)) GiB" >&2
  echo "Estimated need for decompression: $((NEEDED_KB / 1024 / 1024)) GiB" >&2
  echo "Continuing anyway..." >&2
fi

echo "Decompressing to $(pwd)/$TSV"
if [[ "$KEEP_GZ" -eq 1 ]]; then
  gzip -dc "$GZ" > "$TSV"
else
  gzip -d -f "$GZ"
fi

if [[ ! -s "$TSV" ]]; then
  echo "ERROR: decompressed file missing or empty: $(pwd)/$TSV" >&2
  exit 1
fi

echo "Done."
echo "TSV:  $(pwd)/$TSV"
echo "Rows: $(wc -l < "$TSV" | tr -d ' ')"
echo
echo "Example:"
echo "  python3 ingest_chunks.py --file $(pwd)/$TSV --database hits_100b --table hits --parallel 8 --chunk-size 100000 --total-rows 100000000000 --create-sql create.sql --async-insert-busy-timeout-max-ms 5000 --async-insert-max-data-size 10485760"
