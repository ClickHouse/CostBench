#!/bin/bash
# =============================================================================
# Push this repo's quotes/snowflake/ to a box's /home/ubuntu/bench — CODE ONLY.
# Never transfers secrets (keys/, *.pem, *.p8, profile.json, .sfenv, .env,
# _credentials_aws.txt) or local data (out/, results*, *.log, .venv, __pycache__).
# No --delete, so box-local files (.sfenv, keys/, out/, results) are left intact.
#
# DRY-RUN by default; add --apply to actually transfer.
#   bash ops/sync_box.sh ~/.ssh/lio-aws.pem    ubuntu@<paris-host>            # preview
#   bash ops/sync_box.sh ~/.ssh/lio-aws.pem    ubuntu@<paris-host>  --apply   # transfer
#   bash ops/sync_box.sh ~/.ssh/lio-london.pem ubuntu@<london-host> --apply
# =============================================================================
set -euo pipefail
KEY="${1:?usage: sync_box.sh <key.pem> <user@host> [--apply]}"
TARGET="${2:?usage: sync_box.sh <key.pem> <user@host> [--apply]}"
APPLY="${3:-}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/"   # quotes/snowflake/
DRY="-n"; [ "${APPLY}" = "--apply" ] && DRY=""
echo "syncing ${SRC} -> ${TARGET}:/home/ubuntu/bench/   ${DRY:+(DRY-RUN)}"
rsync -avz ${DRY} -e "ssh -i ${KEY} -o StrictHostKeyChecking=accept-new" \
  --exclude '.git' --exclude '.venv/' --exclude '__pycache__/' --exclude '*.pyc' \
  --exclude 'keys/' --exclude '*.pem' --exclude '*.p8' --exclude 'profile.json' \
  --exclude '.sfenv' --exclude '.env' --exclude '_credentials_aws.txt' \
  --exclude 'out/' --exclude 'results*' --exclude '*.log' --exclude '.DS_Store' \
  "${SRC}" "${TARGET}:/home/ubuntu/bench/"
[ -z "${DRY}" ] && echo "SYNCED -> ${TARGET}:/home/ubuntu/bench/" \
                || echo "(dry-run only; re-run with --apply to transfer)"
