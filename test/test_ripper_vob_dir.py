import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


def install_dummy_paho():
    if "paho.mqtt.client" in sys.modules:
        return

    paho_module = types.ModuleType("paho")
    mqtt_module = types.ModuleType("paho.mqtt")
    client_module = types.ModuleType("paho.mqtt.client")

    class DummyClient:
        def __init__(self, **_kwargs):
            pass

        def username_pw_set(self, *_args, **_kwargs):
            pass

        def tls_set(self, *_args, **_kwargs):
            pass

        def connect(self, *_args, **_kwargs):
            pass

        def disconnect(self, *_args, **_kwargs):
            pass

        def publish(self, *_args, **_kwargs):
            pass

    client_module.Client = DummyClient
    client_module.CallbackAPIVersion = types.SimpleNamespace(VERSION2=object())

    paho_module.mqtt = mqtt_module
    mqtt_module.client = client_module

    sys.modules["paho"] = paho_module
    sys.modules["paho.mqtt"] = mqtt_module
    sys.modules["paho.mqtt.client"] = client_module


def load_ripper_module():
    install_dummy_paho()
    ripper_path = Path(__file__).resolve().parents[1] / "ripper" / "ripper.py"
    spec = importlib.util.spec_from_file_location("ripper", ripper_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRipperVobDir(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ripper = load_ripper_module()

    def write_config(self, base: Path) -> Path:
        cfg = base / "ripper.toml"
        cfg.write_text(
            "\n".join(
                [
                    "[mqtt]",
                    'host = "localhost"',
                    "port = 1883",
                    'user = "user"',
                    'password = "pass"',
                    'topic = "media/rip/done"',
                    "ssl = false",
                    "",
                    "[dvd]",
                    'device = "/dev/sr0"',
                    'type = "dvd"',
                    "",
                    "[storage]",
                    f'base_raw = "{base / "raw"}"',
                    'series_path = "Serien"',
                    'movie_path = "Filme"',
                    "",
                    "[heuristics]",
                    "min_episode_minutes = 20",
                    "max_episode_minutes = 60",
                ]
            )
        )
        return cfg

    def test_rejects_iso_and_vob_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            cfg = self.write_config(base)
            argv = [
                "ripper.py",
                "--movie-name",
                "Movie",
                "--iso",
                "/tmp/disc.iso",
                "--vob-dir",
                "/tmp/VIDEO_TS",
                "--config",
                str(cfg),
            ]
            with mock.patch.object(self.ripper.sys, "argv", argv):
                with self.assertRaises(SystemExit):
                    self.ripper.main()

    def test_vob_dir_requires_vob_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            cfg = self.write_config(base)
            vob_dir = base / "VIDEO_TS"
            vob_dir.mkdir()
            argv = [
                "ripper.py",
                "--movie-name",
                "Movie",
                "--vob-dir",
                str(vob_dir),
                "--config",
                str(cfg),
            ]
            with mock.patch.object(self.ripper.sys, "argv", argv):
                with self.assertRaises(SystemExit):
                    self.ripper.main()

    def test_vob_dir_uses_file_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            cfg = self.write_config(base)
            vob_dir = base / "VIDEO_TS"
            vob_dir.mkdir()
            (vob_dir / "VTS_01_1.VOB").write_text("data")

            info_text = 'TINFO:0,9,0,"0:42:00"\n'
            recorded = []

            def fake_run(cmd):
                recorded.append(cmd)
                if cmd[3] == "info":
                    return info_text
                if cmd[3] == "mkv":
                    out_dir = Path(cmd[-1])
                    out_dir.mkdir(parents=True, exist_ok=True)
                    (out_dir / "title_t00.mkv").write_bytes(b"fake")
                return ""

            argv = [
                "ripper.py",
                "--movie-name",
                "Movie",
                "--vob-dir",
                str(vob_dir),
                "--config",
                str(cfg),
            ]

            with mock.patch.object(self.ripper.sys, "argv", argv):
                with mock.patch.object(self.ripper, "run", side_effect=fake_run):
                    with mock.patch.object(self.ripper, "mqtt_test_connection", return_value=False):
                        with mock.patch.object(self.ripper, "mqtt_publish"):
                            with mock.patch.object(self.ripper.time, "sleep"):
                                self.ripper.main()

            self.assertTrue(recorded)
            self.assertEqual(recorded[0][4], f"file:{vob_dir}")
            movie_out = base / "raw" / "dvd" / "Filme" / "Movie.mkv"
            self.assertTrue(movie_out.exists())


if __name__ == "__main__":
    unittest.main()
