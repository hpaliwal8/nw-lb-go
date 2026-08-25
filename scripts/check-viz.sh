#!/usr/bin/env bash
# Build the visualisation and prove it actually renders.
#
# A figure that throws during mount is caught by mountAll and replaced with a "failed to load"
# notice, so a clean DOM dump containing every expected section is real evidence the page works —
# not merely that the file parsed.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/docs/viz/index.html"
SHOT="${1:-$ROOT/docs/viz/preview.png}"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

fail=0

echo "== syntax =="
for f in "$ROOT"/docs/viz/src/core.js "$ROOT"/docs/viz/src/diagrams/*.js; do
  [ -e "$f" ] || continue
  if node --check "$f"; then
    echo "  ok   $(basename "$f")"
  else
    echo "  FAIL $(basename "$f")"; fail=1
  fi
done

echo "== build =="
python3 "$ROOT/scripts/build-viz.py" || fail=1

if [ ! -x "$CHROME" ]; then
  echo "== render == skipped (Chrome not found)"
  exit $fail
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "== render =="
# virtual-time-budget lets the page's rAF loop run before we look at it.
"$CHROME" --headless --disable-gpu --no-sandbox --hide-scrollbars \
  --virtual-time-budget=4000 --window-size=1200,2400 \
  --dump-dom "file://$OUT" > "$TMP/dom.html" 2>"$TMP/err.txt"

if [ ! -s "$TMP/dom.html" ]; then
  echo "  FAIL chrome produced no DOM"; sed -n '1,20p' "$TMP/err.txt"; exit 1
fi

# Inspect the rendered body only. The inlined <script> holds core.js source, which contains the
# very strings we search for, so grepping the raw dump reports failures that never happened.
python3 - "$TMP/dom.html" <<'PY'
import re, sys
dom = open(sys.argv[1], encoding="utf-8", errors="replace").read()
body = re.sub(r"(?is)<script\b.*?</script>", "", dom)
body = re.sub(r"(?is)<style\b.*?</style>", "", body)

sections = len(re.findall(r'<section[^>]*class="figure"', body))
svgs = len(re.findall(r"<svg\b", body))
controls = len(re.findall(r'<button[^>]*class="ctl"', body))
broken = len(re.findall(r"failed to load", body))
ids = re.findall(r'<section[^>]*id="([^"]+)"[^>]*class="figure"', body) or \
      re.findall(r'<section[^>]*class="figure"[^>]*id="([^"]+)"', body)

print(f"  figures mounted : {sections}  {ids}")
print(f"  svg elements    : {svgs}")
print(f"  controls        : {controls}")
print(f"  failed figures  : {broken}")

problems = []
if sections < 1:
    problems.append("no figures mounted")
if broken:
    problems.append(f"{broken} figure(s) threw during mount")
if svgs < sections:
    problems.append("a figure mounted without drawing an svg")
if controls < 1 and sections:
    problems.append("no interactive controls rendered")
for p in problems:
    print(f"  FAIL {p}")
sys.exit(1 if problems else 0)
PY
[ $? -eq 0 ] || fail=1

"$CHROME" --headless --disable-gpu --no-sandbox --hide-scrollbars \
  --virtual-time-budget=4000 --window-size=1200,2400 \
  --screenshot="$SHOT" "file://$OUT" >/dev/null 2>&1

if [ -s "$SHOT" ]; then
  echo "  screenshot      : $SHOT"
else
  echo "  FAIL screenshot not produced"; fail=1
fi

exit $fail
