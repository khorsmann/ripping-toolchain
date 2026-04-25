# Ripping Toolchain

Dieses Projekt automatisiert meinen privaten Workflow zum digitalisieren eigener DVDs. Die Skripte sind ausschließlich für den persönlichen Gebrauch gedacht (Recht auf Privatkopie nach deutschem Recht) und dürfen nicht zum Anfertigen oder Verbreiten unlizenzierter Kopien eingesetzt werden.

> ⚠️ **Hinweis:** Das hier ist stark „gevibedcodeder“ Foo und nicht produktiv gehärtet. Wenn das fliegende Spaghettimonster spontan an der Verkabelung rüttelt, kann jederzeit etwas brechen – Nutzung also auf eigenes Risiko.


## Architektur-Überblick

```
DVD ----> ripper (MakeMKV) --raw MKVs--> /media/raw/(dvd|bluray)
                         |
                         +--> MQTT event media/rip/done

transcode_mqtt (FFmpeg) <--- MQTT subscription
        |
        +--(media/transcode/start|done|error)--> MQTT status
        |
        +--> /media/Serien (HEVC-Transcodes)
```

- **ripper/ripper.py** analysiert eine eingelegte DVD via MakeMKV CLI, wählt anhand einer Kapitel-Dauer-Heuristik geeignete Episoden aus, rippt sie nach `base_raw/<source_type>` und veröffentlicht anschließend ein MQTT-Event (`media/rip/done`), das Pfad, Serie, Staffel, Disc usw. enthält.
- **transcode/transcode_mqtt.py** läuft als Dienst, abonniert das MQTT-Topic `media/rip/done`, queued jedes empfangene Path-Event und transkodiert alle darin enthaltenen `.mkv` Dateien nach `SERIES_DST_BASE` (standardmäßig `/media/Serien`) bzw. `MOVIE_DST_BASE` via `ffmpeg` + VAAPI-Hardwarebeschleunigung. Standardmäßig werden bei Blu-rays Audio-Spuren nach EAC3 (Mehrkanal) bzw. AAC (Stereo) encodiert, DVDs bleiben im Original (`AUDIO_MODE=auto`); Sprache lässt sich über `AUDIO_LANGS`/`SUB_LANGS` filtern. Fortschritt und Fehler werden auf `media/transcode/start`, `media/transcode/done` bzw. `media/transcode/error` zurückgemeldet. Für größere Last kann die interne Queue persistent per SQLite betrieben werden (`JOB_QUEUE_BACKEND=sqlite`).
- Optionale Integrationen (z. B. Home Assistant) können sowohl auf rip- als auch transcode-Topics reagieren, siehe `misc/homeassistant/`.
- **transcode/rescan.py** prüft den Roh-Baum (`SRC_BASE`) gegen die Ziele (`SERIES_DST_BASE`/`MOVIE_DST_BASE`) und sendet MQTT-Jobs für alle Quell-Dirs, in denen transkodierte MKVs fehlen; `--dry-run` zeigt nur an, was gesendet würde. Lädt optional das gleiche Env-File wie der Dienst (`--env-file`, Default `/etc/transcode-mqtt.env`) und kann Publish-Batches drosseln (`--batch-size`, `--batch-sleep` bzw. `RESCAN_BATCH_SIZE`, `RESCAN_BATCH_SLEEP`).


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
  - Optional kann die Quelle statt des Laufwerks als ISO (`--iso /pfad/disc.iso`) oder als VOB-Verzeichnis (`--vob-dir /pfad/VIDEO_TS`) angegeben werden.
  - Optional kann `--interlaced` gesetzt werden, um Deinterlacing zu erzwingen (ohne Wert = `true`, mit Wert `true/false`); ohne Flag bleibt die Entscheidung bei der Auto-Erkennung im Transcode.
  - Die Heuristik erlaubt eine minimale (`min_episode_minutes`) und optional maximale (`max_episode_minutes`) Laufzeit, sodass Komplett-Disc-Titel (z. B. „title 0“ mit allen Episoden) ignoriert werden können; mit `--movie-name <Titel>` lässt sich der Film-Modus aktivieren, bei dem nur die Mindestlaufzeit greift.
  - Im Film-Modus entfallen `--series`, `--season`, `--disc` und `--episode-start`; die Datei wird als `<base_raw>/<source_type>/<movie_path>/<Titel>.mkv` (inkl. Info-Datei) abgelegt – `movie_path` stammt aus der Storage-Config (Default `Filme`), `<Titel>` ist der übergebene (normalisierte) `--movie-name`.
  - Ruft `makemkvcon` (`info` und `mkv`) auf, benennt die erzeugten Dateien um und legt Serien unter `<base_raw>/<source_type>/<series_path>/<Serie>/S<Staffel>/<Disc>` ab (Default `series_path = "Serien"`).
  - Publiziert `version = 1` in jedem `media/rip/done` Payload; transcode-MQTT lehnt andere Versionen strikt ab.
   - Wirft am Ende das Laufwerk aus und publiziert das oben genannte MQTT-Payload.

2. **Transcode-Dienst**  
   - Läuft typischerweise via Systemd (`transcode/transcode-mqtt.service`) und lädt seine Umgebung aus `/etc/transcode-mqtt.env` (nutzt `/usr/lib/jellyfin-ffmpeg/ffmpeg`, falls vorhanden; sonst System-FFmpeg, überschreibbar via `FFMPEG_BIN`/`FFPROBE_BIN`).
   - Sobald ein `media/rip/done`-Event eingeht, landet der Pfad in einer internen Queue. Standard ist RAM-Queue (`JOB_QUEUE_BACKEND=memory`), optional persistente SQLite-Queue (`JOB_QUEUE_BACKEND=sqlite`, `JOB_QUEUE_SQLITE_PATH`), inkl. Reclaim hängender Jobs (`JOB_QUEUE_CLAIM_TTL`). Ein Worker-Thread verarbeitet das Verzeichnis sequenziell:
     - Vor jeder Datei wird `media/transcode/start` inkl. Eingangs- und Ausgabepfad publiziert.
     - Während `ffmpeg` läuft, hält ein Lock unter `/var/lock/vaapi.lock` andere Instanzen von der GPU fern.
     - Hardware-Retries sind über `MAX_HW_RETRIES` konfigurierbar (Default 2 nach dem initialen Versuch).
     - Videoqualität (und Artefakte) ist über `QSV_GLOBAL_QUALITY_*`, `VAAPI_QP_*` und `X265_CRF_*` steuerbar; kleinere Werte bedeuten bessere Qualität bei größerer Dateigröße.
     - Nach erfolgreichem Transcode wird `media/transcode/done` gesendet; Fehler landen auf `media/transcode/error`.
   - Idempotent: existiert die Zielfile bereits, wird sie übersprungen.
   - Serien landen unter `SERIES_DST_BASE` (Default `/media/Serien`) und spiegeln die Struktur unter `SRC_BASE/<SERIES_SUBPATH>` (Standard `Serien`). Filme (`mode=movie`) werden nach `MOVIE_DST_BASE` (Default `/media/Filme`, überschreibbar) abgelegt.
   - Alle Status-Payloads (`media/transcode/*`) enthalten ebenfalls `version = 1`, um Integrationen mit demselben Protokoll zu synchronisieren.
   - Fehlt nachträglich eine transkodierte Datei, kann `transcode/rescan.py` den Raw-Baum erneut scannen und MQTT-Jobs für die fehlenden Ziele senden (`--dry-run` zum Prüfen).


## Abhängigkeiten

### Gemeinsame Anforderungen
- Linux-System mit ausreichend Speicherplatz in den Mounts `/media/raw` und `/media/Serien` (oder angepassten Pfaden).
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

- Beide Komponenten gehen davon aus, dass `series_path` (Ripper) und `SERIES_SUBPATH` (Transcode) identisch sind. Bei `SRC_BASE` ohne Unterordner werden Serien unter `SRC_BASE/<SERIES_SUBPATH>` erwartet; bei `SRC_BASE` mit `dvd/` bzw. `bluray/` darunter liegt die Struktur unter `SRC_BASE/<source_type>/<SERIES_SUBPATH>`. Filme landen gesammelt unter `MOVIE_DST_BASE`.
- MQTT-Topics lassen sich über Environment-Variablen anpassen; Standard ist `media/rip/done` für Eingänge und `media/transcode/*` für Statusmeldungen.
- `rescan.py`-Hilfe anzeigen: `./transcode/rescan.py --help`
- Für große Rescans: z. B. `RESCAN_BATCH_SIZE=3` und `RESCAN_BATCH_SLEEP=0.25`, damit Broker/Consumer nicht mit zu vielen Nachrichten auf einmal geflutet werden.
- Für ausfallsichere Queue im Transcode-Dienst: `JOB_QUEUE_BACKEND=sqlite` und einen persistenten Pfad bei `JOB_QUEUE_SQLITE_PATH` setzen.
- Zum Debuggen von QSV gibt es `misc/qsv-test.sh` (nutzt Jellyfin-FFmpeg, falls vorhanden; sonst System-FFmpeg) und führt kurze Hardware-Encode-Tests für progressiv und Deinterlace aus.
- Zum Korrigieren fehlerhafter Seitenverhältnisse gibt es `misc/fix-aspect.py` (setzt DAR via `mkvpropedit` für explizit angegebene Dateien/Verzeichnisse).
- Der komplette Workflow dient ausschließlich dazu, privat erworbene Medien für den Eigenbedarf zu digitalisieren. Rechte Dritter (DRM, Urheberrecht) sind zu beachten; eine Weitergabe oder öffentliche Bereitstellung gerippter/transkodierter Dateien ist nicht vorgesehen.


## Tests

- Abhängigkeiten installieren/synchronisieren: `uv sync --dev`
- Alle Tests ausführen: `uv run pytest`
- Einzelne Testdatei ausführen: `uv run pytest test/test_ripper_vob_dir.py`
