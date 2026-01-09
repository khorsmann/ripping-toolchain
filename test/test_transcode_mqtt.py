import importlib.util
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
        cls.transcode = load_transcode_module()

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

    def test_probe_audio_channels(self):
        probe_audio_channels = self.transcode.probe_audio_channels
        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.return_value = b"2\n"
            self.assertEqual(probe_audio_channels(Path("dummy.mkv")), 2)
            mocked.return_value = b"6\n"
            self.assertEqual(probe_audio_channels(Path("dummy.mkv")), 6)

        with mock.patch.object(self.transcode.subprocess, "check_output") as mocked:
            mocked.side_effect = OSError("boom")
            self.assertIsNone(probe_audio_channels(Path("dummy.mkv")))

    def test_build_audio_args(self):
        build_audio_args = self.transcode.build_audio_args
        self.assertIn("256k", build_audio_args(2, "dvd"))
        self.assertIn("640k", build_audio_args(6, "dvd"))
        self.assertIn("768k", build_audio_args(6, "bluray"))
        self.assertIn("640k", build_audio_args(None, "dvd"))

    def test_build_downmix_args(self):
        args = self.transcode.build_downmix_args()
        self.assertIn("aac", args)
        self.assertIn("192k", args)
        self.assertIn("2", args)

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


if __name__ == "__main__":
    unittest.main()
