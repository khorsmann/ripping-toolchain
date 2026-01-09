import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_rescan_module():
    rescan_path = Path(__file__).resolve().parents[1] / "transcode" / "rescan.py"
    spec = importlib.util.spec_from_file_location("rescan", rescan_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRescanHelpers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rescan = load_rescan_module()

    def test_parse_source_type(self):
        parse_source_type = self.rescan.parse_source_type
        self.assertEqual(parse_source_type("dvd"), "dvd")
        self.assertEqual(parse_source_type("BluRay"), "bluray")
        self.assertIsNone(parse_source_type("unknown"))
        self.assertIsNone(parse_source_type(""))

    def test_find_source_type_marker(self):
        find_source_type_marker = self.rescan.find_source_type_marker
        parse_source_type = self.rescan.parse_source_type
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            leaf = base / "a" / "b"
            leaf.mkdir(parents=True)
            marker = base / ".source_type"
            marker.write_text("bluray")
            result = find_source_type_marker(leaf, base)
            self.assertEqual(result, parse_source_type("bluray"))

    def test_probe_source_type(self):
        probe_source_type = self.rescan.probe_source_type
        with mock.patch.object(self.rescan.subprocess, "check_output") as mocked:
            mocked.return_value = b"576\n"
            self.assertEqual(probe_source_type(Path("dummy.mkv")), "dvd")
            mocked.return_value = b"1080\n"
            self.assertEqual(probe_source_type(Path("dummy.mkv")), "bluray")
            mocked.return_value = b"640\n"
            self.assertIsNone(probe_source_type(Path("dummy.mkv")))

    def test_detect_source_type_prefers_marker_then_probe_then_fallback(self):
        detect_source_type = self.rescan.detect_source_type
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            leaf = base / "a"
            leaf.mkdir()
            (base / ".source_type").write_text("dvd")
            with mock.patch.object(self.rescan, "probe_source_type") as mocked:
                mocked.return_value = "bluray"
                result = detect_source_type(leaf, base, "bluray", Path("dummy.mkv"))
                self.assertEqual(result, "dvd")
                mocked.assert_not_called()

            (base / ".source_type").unlink()
            with mock.patch.object(self.rescan, "probe_source_type") as mocked:
                mocked.return_value = "bluray"
                result = detect_source_type(leaf, base, "dvd", Path("dummy.mkv"))
                self.assertEqual(result, "bluray")

            with mock.patch.object(self.rescan, "probe_source_type") as mocked:
                mocked.return_value = None
                result = detect_source_type(leaf, base, "dvd", Path("dummy.mkv"))
                self.assertEqual(result, "dvd")

    def test_main_builds_v3_payloads(self):
        rescan = self.rescan
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            src_base = base / "raw"
            series_src = src_base / "Serien" / "Show" / "S01" / "disc01"
            movie_src = src_base / "Filme" / "MovieA"
            series_src.mkdir(parents=True)
            movie_src.mkdir(parents=True)

            series_mkv = series_src / "Show-S01E01.mkv"
            movie_mkv = movie_src / "MovieA.mkv"
            series_mkv.write_bytes(b"fake")
            movie_mkv.write_bytes(b"fake")

            series_dst = base / "out" / "Serien"
            movie_dst = base / "out" / "Filme"
            series_dst.mkdir(parents=True)
            movie_dst.mkdir(parents=True)

            env = {
                "MQTT_HOST": "localhost",
                "MQTT_USER": "user",
                "MQTT_PASSWORD": "pass",
                "SRC_BASE": str(src_base),
                "SERIES_SUBPATH": "Serien",
                "MOVIE_SUBPATH": "Filme",
                "SERIES_DST_BASE": str(series_dst),
                "MOVIE_DST_BASE": str(movie_dst),
                "SOURCE_TYPE": "dvd",
            }

            payloads = []

            def fake_publish(client, topic, payload, dry_run):
                payloads.append(payload)

            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(rescan, "build_mqtt_client") as build_client:
                    build_client.return_value = object()
                    with mock.patch.object(rescan, "connect_mqtt"):
                        with mock.patch.object(rescan, "mqtt_publish", side_effect=fake_publish):
                            with mock.patch.object(rescan, "detect_source_type") as detect_type:
                                detect_type.return_value = "dvd"
                                with mock.patch.object(
                                    rescan, "load_env_file"
                                ) as load_env_file:
                                    load_env_file.return_value = None
                                    with mock.patch.object(rescan.sys, "argv", ["rescan.py"]):
                                        rescan.main()

            self.assertEqual(len(payloads), 2)
            for payload in payloads:
                self.assertEqual(payload["version"], 3)
                self.assertIn(payload["mode"], {"movie", "series"})
                self.assertEqual(payload["source_type"], "dvd")
                self.assertIsNone(payload["interlaced"])
                self.assertTrue(payload["files"])
                for item in payload["files"]:
                    self.assertTrue(Path(item).is_absolute())


if __name__ == "__main__":
    unittest.main()
