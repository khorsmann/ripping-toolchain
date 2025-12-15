# Home Assistant Automation

Dieses Beispiel zeigt, wie du in Home Assistant eine Push-Benachrichtigung bekommst, sobald der Ripper das MQTT-Event `media/rip/done` sendet.

## Verwendung

1. Kopiere `automations.yml` (oder den relevanten Abschnitt) in deine bestehende `automations.yaml` innerhalb von Home Assistant.
2. Passe den Service `notify.mobile_app_meinphone` an den Namen deiner eigenen Companion-App an (z. B. `notify.mobile_app_pixel_7`).
3. Optional: Ändere Topic oder Nachrichtentext, falls dein Setup andere Topics oder Sprachen nutzt.
4. Lade die Automationen neu (`Einstellungen → Automatisierungen & Szenen → Automationen neu laden`) oder starte Home Assistant neu.

Sobald der Ripper eine Disc fertig meldet, sendet Home Assistant dir eine Push-Nachricht mit Serien-, Staffel-, Disc- und Episoden-Informationen sowie dem Speicherpfad.
