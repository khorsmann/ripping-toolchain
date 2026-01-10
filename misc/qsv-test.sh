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

echo "== QSV smoke test (progressive) =="
set +e
"${FFMPEG_BIN}" \
  -hwaccel qsv \
  -qsv_device /dev/dri/renderD128 \
  -hwaccel_output_format qsv \
  -f lavfi -i testsrc2=size=1280x720:rate=30 \
  -vf "vpp_qsv=deinterlace=0" \
  -t 2 \
  -c:v h264_qsv \
  -f null -
status=$?
set -e

if [[ $status -eq 0 ]]; then
  echo "RESULT: QSV OK"
else
  echo "RESULT: QSV FAILED (exit ${status})"
  echo "HINT: Check /dev/dri/renderD128 permissions and QSV runtime availability."
fi
echo

echo "== QSV smoke test (deinterlace) =="
set +e
"${FFMPEG_BIN}" \
  -hwaccel qsv \
  -qsv_device /dev/dri/renderD128 \
  -hwaccel_output_format qsv \
  -f lavfi -i testsrc2=size=1280x720:rate=30 \
  -vf "vpp_qsv=deinterlace=1" \
  -t 2 \
  -c:v h264_qsv \
  -f null -
status2=$?
set -e

if [[ $status2 -eq 0 ]]; then
  echo "RESULT: QSV+DEINT OK"
else
  echo "RESULT: QSV+DEINT FAILED (exit ${status2})"
  echo "HINT: Check /dev/dri/renderD128 permissions and QSV runtime availability."
fi

if [[ $status -ne 0 || $status2 -ne 0 ]]; then
  exit 1
fi
exit 0
