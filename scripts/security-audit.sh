#!/usr/bin/env bash
# Supply-chain audit: checks every service's installed dependencies against
# known-vulnerability databases and saves the reports in docs/security/.
#   Python services -> pip-audit (PyPI advisory DB + OSV), run inside each built image
#   Frontend        -> npm audit (GitHub advisory DB)
# Usage: ./scripts/security-audit.sh   (run from the repository root, images already built)
set -uo pipefail

OUT=docs/security
mkdir -p "$OUT"
DATE=$(date -u +"%Y-%m-%d %H:%M UTC")
SUMMARY="$OUT/README.md"

{
  echo "# Dependency audit report"
  echo
  echo "Generated: $DATE"
  echo
  echo "| Component | Tool | Result | Report |"
  echo "|---|---|---|---|"
} > "$SUMMARY"

PY_SERVICES=(gateway orders-service flights-service hotels-service cars-service mock-provider pipeline)
for svc in "${PY_SERVICES[@]}"; do
  echo "Auditing $svc ..."
  report="$OUT/pip-audit-$svc.md"
  # stdout = the report; Docker's own progress messages (stderr) are discarded
  docker compose run --rm --no-deps -T --entrypoint sh "$svc" -c \
    'pip install --user -q --disable-pip-version-check pip-audit >/dev/null 2>&1 && ~/.local/bin/pip-audit --format markdown --progress-spinner off 2>&1' \
    > "$report" 2>/dev/null
  status=$?
  if [ $status -eq 0 ]; then
    result="No known vulnerabilities"
    echo "No known vulnerabilities found (pip-audit, $DATE)." >> "$report"
  elif [ -s "$report" ]; then
    result="**Findings, see report**"
  else
    result="**Audit could not run**"
  fi
  echo "| $svc | pip-audit | $result | [$(basename "$report")]($(basename "$report")) |" >> "$SUMMARY"
done

echo "Auditing frontend ..."
report="$OUT/npm-audit-frontend.txt"
docker run --rm -v "$PWD/frontend:/app:ro" -w /app -e npm_config_update_notifier=false node:22-alpine npm audit > "$report" 2>/dev/null
status=$?
if [ $status -eq 0 ]; then result="No known vulnerabilities"; else result="**Findings, see report**"; fi
echo "| frontend | npm audit | $result | [$(basename "$report")]($(basename "$report")) |" >> "$SUMMARY"

echo
cat "$SUMMARY"
