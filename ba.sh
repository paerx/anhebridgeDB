#!/usr/bin/env bash
set -euo pipefail

FILE="1"
ADDR="https://anhe.zeabur.app"
USER="admin"
PASS="ansdh8weiwfheeb8fiwehbsyudie94wefhidscxuyeq"
BATCH=1000

total=$(wc -l < "$FILE" | tr -d ' ')
echo "total lines: $total"

batch_no=0
dsl="ASET "
count=0

while IFS= read -r url || [[ -n "$url" ]]; do
  [[ -z "$url" ]] && continue
  code="${url##*/}"
  item="vape_chest_qrcode:${code} \"\""

  if [[ "$dsl" != "ASET " ]]; then
    dsl+=", "
  fi
  dsl+="$item"

  count=$((count + 1))
  if (( count % BATCH == 0 )); then
    batch_no=$((batch_no + 1))
    echo "run batch: $batch_no"
 go run cmd/client/main.go -addr "$ADDR" -u "$USER" -a "$PASS" -e "${dsl};"
    dsl="ASET "
  fi
done < "$FILE"

if [[ "$dsl" != "ASET " ]]; then
  batch_no=$((batch_no + 1))
  echo "run batch: $batch_no (last partial)"

  go run cmd/client/main.go -addr "$ADDR" -u "$USER" -a "$PASS" -e "${dsl};"
fi

echo "done"

