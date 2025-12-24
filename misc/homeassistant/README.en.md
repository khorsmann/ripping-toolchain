# Home Assistant Automation

This example shows how to receive a push notification in Home Assistant as soon as the ripper sends the MQTT event `media/rip/done`.

## Usage

1. Copy `automations.yml` (or the relevant section) into your existing `automations.yaml` within Home Assistant.
2. Adjust the service `notify.mobile_app_meinphone` to the name of your own companion app (e.g., `notify.mobile_app_pixel_7`).
3. Optional: change topic or message text if your setup uses different topics or languages.
4. Reload automations (`Settings → Automations & Scenes → Reload Automations`) or restart Home Assistant.

Once the ripper reports a finished disc, Home Assistant sends you a push notification with series, season, disc, episode information, and the storage path.
