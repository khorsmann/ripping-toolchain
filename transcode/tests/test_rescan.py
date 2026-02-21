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

    def test_main_dry_run_skips_mqtt_connection(self):
        mod = load_rescan_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            src = base / "raw"
            movie_src = src / "Filme" / "Movie (2024) & Co"
            movie_dst = base / "dst-movie"
            series_dst = base / "dst-series"
            movie_src.mkdir(parents=True, exist_ok=True)
            movie_dst.mkdir(parents=True, exist_ok=True)
            series_dst.mkdir(parents=True, exist_ok=True)

            (movie_src / "Movie (2024) & Co.mkv").touch()

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

            def fake_publish(client, _topic, payload, dry_run):
                publishes.append((client, payload, dry_run))

            argv = [
                "rescan.py",
                "--dry-run",
                "--env-file",
                str(base / "missing.env"),
                "--batch-sleep",
                "0.25",
            ]
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch("sys.argv", argv),
                mock.patch.object(mod, "build_mqtt_client") as build_mock,
                mock.patch.object(mod, "connect_mqtt") as connect_mock,
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

            build_mock.assert_not_called()
            connect_mock.assert_not_called()
            sleep_mock.assert_not_called()
            self.assertEqual(len(publishes), 1)
            self.assertIsNone(publishes[0][0])
            self.assertTrue(publishes[0][2])

    def test_collect_missing_movie_dirs_handles_nested_dirs_and_special_chars(self):
        mod = load_rescan_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            movie_src = base / "raw" / "Filme"
            movie_dst = base / "dst-filme"
            nested = movie_src / "Klassiker ÄÖÜ" / "Disc 01"
            nested.mkdir(parents=True, exist_ok=True)
            movie_dst.mkdir(parents=True, exist_ok=True)

            src_mkv = nested / "Léon - Der Profi.mkv"
            src_mkv.touch()

            # Matches transcode_mqtt movie behavior: output is flattened to basename.
            flattened_output = movie_dst / src_mkv.name
            flattened_output.touch()

            missing, skipped = mod.collect_missing_movie_dirs(movie_src, movie_dst)
            self.assertEqual(missing, {})
            self.assertEqual(skipped, [])

    def test_collect_missing_movie_dirs_accepts_legacy_relative_output(self):
        mod = load_rescan_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            movie_src = base / "raw" / "Filme"
            movie_dst = base / "dst-filme"
            nested = movie_src / "Action" / "Movie"
            nested.mkdir(parents=True, exist_ok=True)
            movie_dst.mkdir(parents=True, exist_ok=True)

            src_mkv = nested / "Movie 01.mkv"
            src_mkv.touch()

            # Legacy/manual layout preserving movie source subfolders.
            legacy_output = movie_dst / "Action" / "Movie" / "Movie 01.mkv"
            legacy_output.parent.mkdir(parents=True, exist_ok=True)
            legacy_output.touch()

            missing, skipped = mod.collect_missing_movie_dirs(movie_src, movie_dst)
            self.assertEqual(missing, {})
            self.assertEqual(skipped, [])

    def test_collect_missing_movie_dirs_handles_parentheses_and_ampersand(self):
        mod = load_rescan_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            movie_src = base / "raw" / "Filme"
            movie_dst = base / "dst-filme"
            nested = movie_src / "Action & Abenteuer (1985)" / "Disc (01)"
            nested.mkdir(parents=True, exist_ok=True)
            movie_dst.mkdir(parents=True, exist_ok=True)

            src_mkv = nested / "Tom & Jerry (Der Film).mkv"
            src_mkv.touch()

            flattened_output = movie_dst / src_mkv.name
            flattened_output.touch()

            missing, skipped = mod.collect_missing_movie_dirs(movie_src, movie_dst)
            self.assertEqual(missing, {})
            self.assertEqual(skipped, [])


if __name__ == "__main__":
    unittest.main()
