# Ripping Toolchain

Dieses Projekt automatisiert meinen privaten Workflow zum digitalisieren eigener DVDs. Die Skripte sind ausschließlich für den persönlichen Gebrauch gedacht (Recht auf Privatkopie nach deutschem Recht) und dürfen nicht zum Anfertigen oder Verbreiten unlizenzierter Kopien eingesetzt werden.

> ⚠️ **Hinweis:** Das hier ist stark „gevibedcodeder“ Foo und nicht produktiv gehärtet. Wenn das fliegende Spaghettimonster spontan an der Verkabelung rüttelt, kann jederzeit etwas brechen – Nutzung also auf eigenes Risiko.


## Architektur-Überblick

```
DVD ----> ripper (MakeMKV) --raw MKVs--> /media/raw/dvd
                         |
                         +--> MQTT event media/rip/done

transcode_mqtt (FFmpeg) <--- MQTT subscription
        |
        +--(media/transcode/start|done|error)--> MQTT status
        |
        +--> /media/Serien (HEVC-Transcodes)
```

- **ripper/ripper.py** analysiert eine eingelegte DVD via MakeMKV CLI, wählt anhand einer Kapitel-Dauer-Heuristik geeignete Episoden aus, rippt sie nach `base_raw` und veröffentlicht anschließend ein MQTT-Event (`media/rip/done`), das Pfad, Serie, Staffel, Disc usw. enthält.
- **transcode/transcode_mqtt.py** läuft als Dienst, abonniert das MQTT-Topic `media/rip/done`, queued jedes empfangene Path-Event und transkodiert alle darin enthaltenen `.mkv` Dateien nach `DST_BASE` (standardmäßig `/media/Serien`) via `ffmpeg` + VAAPI-Hardwarebeschleunigung. Fortschritt und Fehler werden auf `media/transcode/start`, `media/transcode/done` bzw. `media/transcode/error` zurückgemeldet.
- Optionale Integrationen (z. B. Home Assistant) können sowohl auf rip- als auch transcode-Topics reagieren, siehe `misc/homeassistant/`.


## Komponenten & Zusammenspiel

1. **Ripper starten**  
   ```
   ./ripper/ripper.py \
     --series Dein_toller_Serietitle \
     --season 02 \
     --disc disc07 \
     --episode-start 23 \
     --config ripper/ripper.toml
   ```
  - Liest `ripper.toml` (MQTT, DVD-Gerät, Storage, Heuristik) und prüft zuerst die MQTT-Konnektivität.
  - Die Heuristik erlaubt eine minimale (`min_episode_minutes`) und optional maximale (`max_episode_minutes`) Laufzeit, sodass Komplett-Disc-Titel (z. B. „title 0“ mit allen Episoden) ignoriert werden können; mit `--movie-name <Titel>` lässt sich der Film-Modus aktivieren, bei dem nur die Mindestlaufzeit greift.
  - Im Film-Modus entfallen `--series`, `--season`, `--disc` und `--episode-start`; die Datei wird als `<base_raw>/<movie_path>/<Titel>.mkv` (inkl. Info-Datei) abgelegt – `movie_path` stammt aus der Storage-Config, `<Titel>` ist der übergebene (normalisierte) `--movie-name`.
  - Ruft `makemkvcon` (`info` und `mkv`) auf, benennt die erzeugten Dateien um und legt alles unter `/media/raw/dvd/<Serie>/S<Staffel>/<Disc>` ab.
  - Publiziert `version = 1` in jedem `media/rip/done` Payload; transcode-MQTT lehnt andere Versionen strikt ab.
   - Wirft am Ende das Laufwerk aus und publiziert das oben genannte MQTT-Payload.

2. **Transcode-Dienst**  
   - Läuft typischerweise via Systemd (`transcode/transcode-mqtt.service`) und lädt seine Umgebung aus `/etc/transcode-mqtt.env`.
   - Sobald ein `media/rip/done`-Event eingeht, landet der Pfad in einer internen Queue. Ein Worker-Thread verarbeitet das Verzeichnis sequenziell:
     - Vor jeder Datei wird `media/transcode/start` inkl. Eingangs- und Ausgabepfad publiziert.
     - Während `ffmpeg` läuft, hält ein Lock unter `/var/lock/vaapi.lock` andere Instanzen von der GPU fern.
     - Nach erfolgreichem Transcode wird `media/transcode/done` gesendet; Fehler landen auf `media/transcode/error`.
   - Idempotent: existiert die Zielfile bereits, wird sie übersprungen.
   - Serien landen unter `DST_BASE` (z. B. `/media/Serien`), während Filme (`mode=movie`) automatisch nach `MOVIE_DST_BASE` (Standard: `/media/Filme`, überschreibbar) geschrieben werden.
   - Alle Status-Payloads (`media/transcode/*`) enthalten ebenfalls `version = 1`, um Integrationen mit demselben Protokoll zu synchronisieren.


## Abhängigkeiten

### Gemeinsame Anforderungen
- Linux-System mit ausreichend Speicherplatz in den Mounts `/media/raw/dvd` und `/media/Serien` (oder angepassten Pfaden).
- Laufender **Mosquitto**-Broker (oder kompatibel) mit Benutzer/Passwort für den Ripper und den Transcode-Dienst.
- Python ≥ 3.11 inkl. Module:
  - `paho-mqtt`
  - `tomllib` (ab 3.11 eingebaut; sonst `tomli`)

### Ripper-spezifisch
- **MakeMKV CLI** (`makemkvcon`) inklusive gültiger Lizenz/key.
- Zugriff auf das DVD-Laufwerk (z. B. `/dev/sr0`) und das `eject`-Kommando.
- Optional: Shell-Tools wie `mosquitto_pub` zum manuellen Testen (`test/`-Skripte).

### Transcode-spezifisch
- **FFmpeg** mit VAAPI-Unterstützung und Zugriff auf die GPU (`/dev/dri/renderD128`).
- Schreibrechte für `/var/lock` (für `vaapi.lock`) und das konfigurierte Zielspeicherverzeichnis.
- Optionale Logrotation für `/var/log/transcode-mqtt.log`, falls der Dienst via Systemd-Unit betrieben wird.


## Betriebshinweise

- Beide Komponenten gehen davon aus, dass `SRC_BASE` und `DST_BASE` denselben Verzeichnisbaumstruktur aufweisen (Serie/Staffel/Disc). Änderungen an der Benennung müssen konsistent in beiden Configs geschehen. Filme sind davon ausgenommen und landen gesammelt unter `MOVIE_DST_BASE`.
- MQTT-Topics lassen sich über Environment-Variablen anpassen; Standard ist `media/rip/done` für Eingänge und `media/transcode/*` für Statusmeldungen.
- Der komplette Workflow dient ausschließlich dazu, privat erworbene Medien für den Eigenbedarf zu digitalisieren. Rechte Dritter (DRM, Urheberrecht) sind zu beachten; eine Weitergabe oder öffentliche Bereitstellung gerippter/transkodierter Dateien ist nicht vorgesehen.
