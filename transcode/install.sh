#!/usr/bin/env bash
set -euo pipefail

SERVICE=transcode-mqtt.service
LOGROTATE_SERVICE=transcode-mqtt-logrotate.service
LOGROTATE_TIMER=transcode-mqtt-logrotate.timer
LOCK_FILE=/var/lock/vaapi.lock
SVC_BIN=/usr/local/bin/transcode_mqtt.py

echo "Waiting for transcode to be idle (no lock, no ffmpeg child)..."
while :; do
  lock_busy=false
  if fuser "$LOCK_FILE" >/dev/null 2>&1; then
    lock_busy=true
  fi

  main_pid=$(systemctl show -p MainPID --value "$SERVICE" 2>/dev/null || echo 0)
  ffmpeg_busy=false
  if [[ "$main_pid" != "0" && "$main_pid" != "" ]]; then
    if pgrep -P "$main_pid" -x ffmpeg >/dev/null 2>&1; then
      ffmpeg_busy=true
    fi
  fi

  if [[ "$lock_busy" == false && "$ffmpeg_busy" == false ]]; then
    break
  fi

  echo "  busy (lock=$lock_busy ffmpeg=$ffmpeg_busy); sleeping 5s..."
  sleep 5
done

echo "Stopping $SERVICE..."
if systemctl is-active --quiet "$SERVICE"; then
  systemctl stop "$SERVICE"
else
  echo "  service not active; skipping stop"
fi

echo "Installing files..."
install -o root -g root -m 0755 transcode-mqtt.service /etc/systemd/system/transcode-mqtt.service
install -o root -g root -m 0644 transcode-mqtt-logrotate.service /etc/systemd/system/transcode-mqtt-logrotate.service
install -o root -g root -m 0644 transcode-mqtt-logrotate.timer /etc/systemd/system/transcode-mqtt-logrotate.timer
install -o root -g root -m 0755 transcode_mqtt.py "$SVC_BIN"
install -o root -g root -m 0644 etc/transcode-mqtt.env /etc/transcode-mqtt.env
install -o root -g root -m 0644 etc/transcode-mqtt.logrotate /etc/logrotate.d/transcode-mqtt

echo "Reloading systemd daemon..."
systemctl daemon-reload

echo "Enabling log rotation timer..."
systemctl enable --now "$LOGROTATE_TIMER"

echo "Starting $SERVICE..."
systemctl start "$SERVICE"

echo "Done."
