#!/usr/bin/env bash
install -o root -g root -m 0755 transcode-mqtt.service /etc/systemd/system/transcode-mqtt.service
install -o root -g root -m 0755 transcode_mqtt.py /usr/local/bin/transcode_mqtt.py
install -o root -g root -m 0600 etc/transcode-mqtt.env /etc/transcode-mqtt.env
systemctl daemon-reload
systemctl restart transcode-mqtt.service
