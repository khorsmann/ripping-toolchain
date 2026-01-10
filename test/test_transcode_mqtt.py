import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_transcode_module():
    module_path = Path(__file__).resolve().parents[1] / "transcode" / "transcode_mqtt.py"
    spec = importlib.util.spec_from_file_location("transcode_mqtt", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTranscodeHelpers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._env_backup = dict(os.environ)
        cls._tmpdir = tempfile.TemporaryDirectory()
        os.environ.setdefault("MQTT_HOST", "localhost")
        os.environ.setdefault("MQTT_USER", "user")
        os.environ.setdefault("MQTT_PASSWORD", "pass")
        os.environ.setdefault("SRC_BASE", cls._tmpdir.name)
        cls.transcode = load_transcode_module()

    @classmethod
    def tearDownClass(cls):
        os.environ.clear()
        os.environ.update(cls._env_backup)
        cls._tmpdir.cleanup()

    def test_detect_interlaced(self):
        detect_interlaced = self.transcode.detect_interlaced
        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.return_value = b"tt\n"
            self.assertTrue(detect_interlaced(Path("dummy.mkv")))
            mocked.return_value = b"progressive\n"
            self.assertFalse(detect_interlaced(Path("dummy.mkv")))

        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.side_effect = OSError("boom")
            self.assertIsNone(detect_interlaced(Path("dummy.mkv")))

    def test_probe_audio_streams(self):
        probe_audio_streams = self.transcode.probe_audio_streams
        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.return_value = (
                b'{"streams":[{"index":1,"channels":6,"tags":{"language":"eng"}}]}'
            )
            self.assertEqual(
                probe_audio_streams(Path("dummy.mkv")),
                [{"index": 1, "channels": 6, "language": "eng"}],
            )

        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.side_effect = OSError("boom")
            self.assertEqual(probe_audio_streams(Path("dummy.mkv")), [])

    def test_build_audio_args(self):
        build_audio_args = self.transcode.build_audio_args
        self.assertIn("256k", build_audio_args(0, 2, "dvd"))
        self.assertIn("640k", build_audio_args(0, 6, "dvd"))
        self.assertIn("768k", build_audio_args(0, 6, "bluray"))
        self.assertIn("640k", build_audio_args(0, None, "dvd"))

    def test_build_downmix_args(self):
        args = self.transcode.build_downmix_args(1)
        self.assertIn("aac", args)
        self.assertIn("192k", args)
        self.assertIn("2", args)

    def test_parse_langs(self):
        parse_langs = self.transcode.parse_langs
        self.assertEqual(parse_langs("eng,ger", "eng"), {"eng", "ger"})
        self.assertEqual(parse_langs("", "eng"), {"eng"})
        self.assertEqual(parse_langs(None, "eng,deu"), {"eng", "deu"})

    def test_filter_streams_by_language(self):
        filter_streams = self.transcode.filter_streams_by_language
        streams = [
            {"index": 0, "language": "eng"},
            {"index": 1, "language": "ger"},
            {"index": 2, "language": None},
        ]
        self.assertEqual(
            filter_streams(streams, {"eng"}), [{"index": 0, "language": "eng"}]
        )
        self.assertEqual(filter_streams(streams, set()), streams)
    def test_build_video_filter(self):
        build_video_filter = self.transcode.build_video_filter
        self.assertEqual(
            build_video_filter(True, True),
            "bwdif,format=p010le,hwupload=extra_hw_frames=64",
        )
        self.assertEqual(
            build_video_filter(None, True),
            "format=p010le,hwupload=extra_hw_frames=64",
        )
        self.assertIsNone(build_video_filter(False, False))

    def test_build_sw_filter(self):
        build_sw_filter = self.transcode.build_sw_filter
        self.assertEqual(build_sw_filter(True), "bwdif")
        self.assertIsNone(build_sw_filter(False))
        self.assertIsNone(build_sw_filter(None))

    def test_probe_video_codec(self):
        probe_video_codec = self.transcode.probe_video_codec
        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.return_value = b"vc1\n"
            self.assertEqual(probe_video_codec(Path("dummy.mkv")), "vc1")

        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.side_effect = OSError("boom")
            self.assertIsNone(probe_video_codec(Path("dummy.mkv")))

    def test_audio_mode_default_copy(self):
        self.assertEqual(self.transcode.AUDIO_MODE, "auto")

    def test_resolve_ffmpeg_bin_prefers_env(self):
        transcode = self.transcode
        with tempfile.TemporaryDirectory() as tmpdir:
            fake = Path(tmpdir) / "ffmpeg"
            fake.write_text("")
            os.environ["FFMPEG_BIN"] = str(fake)
            try:
                self.assertEqual(transcode.resolve_ffmpeg_bin(), str(fake))
            finally:
                del os.environ["FFMPEG_BIN"]

    def test_series_src_base_for_source(self):
        transcode = self.transcode
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "dvd" / "Serien").mkdir(parents=True)
            (base / "bluray" / "Serien").mkdir(parents=True)

            old_src_base = transcode.SRC_BASE
            old_series_subpath = transcode.SERIES_SUBPATH
            old_series_src_base = transcode.SERIES_SRC_BASE
            try:
                transcode.SRC_BASE = base
                transcode.SERIES_SUBPATH = Path("Serien")
                transcode.SERIES_SRC_BASE = (base / "Serien").resolve()

                self.assertEqual(
                    transcode.series_src_base_for_source("dvd"),
                    (base / "dvd" / "Serien").resolve(),
                )
                self.assertEqual(
                    transcode.series_src_base_for_source("bluray"),
                    (base / "bluray" / "Serien").resolve(),
                )
                self.assertEqual(
                    transcode.series_src_base_for_source("unknown"),
                    (base / "Serien").resolve(),
                )
            finally:
                transcode.SRC_BASE = old_src_base
                transcode.SERIES_SUBPATH = old_series_subpath
                transcode.SERIES_SRC_BASE = old_series_src_base


if __name__ == "__main__":
    unittest.main()
