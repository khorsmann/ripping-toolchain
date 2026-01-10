#!/usr/bin/env bash
set -euo pipefail

FFMPEG_BIN="${FFMPEG_BIN:-}"
if [[ -z "${FFMPEG_BIN}" ]]; then
  if [[ -x /usr/lib/jellyfin-ffmpeg/ffmpeg ]]; then
    FFMPEG_BIN="/usr/lib/jellyfin-ffmpeg/ffmpeg"
  else
    FFMPEG_BIN="ffmpeg"
  fi
fi

echo "Using ffmpeg: ${FFMPEG_BIN}"
echo "User: $(id)"
echo "DRI nodes:"
ls -l /dev/dri || true
echo

echo "== vainfo =="
if command -v vainfo >/dev/null 2>&1; then
  vainfo || true
else
  echo "vainfo not installed"
fi
echo

echo "== ffmpeg encoders (qsv) =="
"${FFMPEG_BIN}" -encoders | grep -i qsv || true
echo

echo "== ffmpeg hwaccels =="
"${FFMPEG_BIN}" -hwaccels || true
echo

echo "== QSV smoke test =="
set +e
"${FFMPEG_BIN}" \
  -init_hw_device qsv=hw:/dev/dri/renderD128 \
  -filter_hw_device hw \
  -f lavfi -i testsrc2=size=1280x720:rate=30 \
  -t 2 \
  -c:v h264_qsv \
  -f null -
status=$?
set -e

if [[ $status -eq 0 ]]; then
  echo "RESULT: QSV OK"
  exit 0
fi

echo "RESULT: QSV FAILED (exit ${status})"
echo "HINT: Check /dev/dri/renderD128 permissions and QSV runtime availability."
exit "${status}"
