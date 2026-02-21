import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_rescan_module():
    module_path = Path(__file__).resolve().parents[1] / "rescan.py"
    spec = importlib.util.spec_from_file_location("transcode_rescan_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class DummyClient:
    def disconnect(self):
        return None


class RescanTests(unittest.TestCase):
    def test_sleep_between_batches_skips_on_dry_run(self):
        mod = load_rescan_module()
        with mock.patch.object(mod.time, "sleep") as sleep_mock:
            mod.sleep_between_batches(0.5, dry_run=True)
            sleep_mock.assert_not_called()

    def test_main_respects_batch_size_and_sleep(self):
        mod = load_rescan_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            src = base / "raw"
            series_src = src / "Serien" / "Show" / "S01" / "disc01"
            movie_src = src / "Filme" / "Movie"
            series_dst = base / "dst-series"
            movie_dst = base / "dst-movie"
            series_src.mkdir(parents=True, exist_ok=True)
            movie_src.mkdir(parents=True, exist_ok=True)
            series_dst.mkdir(parents=True, exist_ok=True)
            movie_dst.mkdir(parents=True, exist_ok=True)

            for idx in range(4):
                (series_src / f"ep{idx:02d}.mkv").touch()
                (movie_src / f"movie{idx:02d}.mkv").touch()

            env = {
                "MQTT_HOST": "localhost",
                "MQTT_USER": "user",
                "MQTT_PASSWORD": "pass",
                "SRC_BASE": str(src),
                "SOURCE_TYPE": "dvd",
                "SERIES_SUBPATH": "Serien",
                "MOVIE_SUBPATH": "Filme",
                "SERIES_DST_BASE": str(series_dst),
                "MOVIE_DST_BASE": str(movie_dst),
            }
            publishes = []

            def fake_publish(_client, _topic, payload, _dry_run):
                publishes.append(payload)

            argv = [
                "rescan.py",
                "--env-file",
                str(base / "missing.env"),
                "--batch-size",
                "2",
                "--batch-sleep",
                "0.25",
            ]
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch("sys.argv", argv),
                mock.patch.object(mod, "build_mqtt_client", return_value=DummyClient()),
                mock.patch.object(mod, "connect_mqtt"),
                mock.patch.object(mod, "detect_source_type", return_value="dvd"),
                mock.patch.object(
                    mod,
                    "filter_ready_mkvs",
                    side_effect=lambda mkvs, _allow: (mkvs, [], 1080),
                ),
                mock.patch.object(mod, "mqtt_publish", side_effect=fake_publish),
                mock.patch.object(mod.time, "sleep") as sleep_mock,
            ):
                mod.main()

            self.assertEqual(len(publishes), 4)
            self.assertEqual([len(p["files"]) for p in publishes], [2, 2, 2, 2])
            self.assertEqual(sleep_mock.call_count, 4)


if __name__ == "__main__":
    unittest.main()
