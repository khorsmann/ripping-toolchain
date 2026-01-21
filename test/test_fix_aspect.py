import argparse
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_fix_aspect_module():
    fix_path = Path(__file__).resolve().parents[1] / "misc" / "fix-aspect.py"
    spec = importlib.util.spec_from_file_location("fix_aspect", fix_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestFixAspect(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fix = load_fix_aspect_module()

    def test_parse_aspect(self):
        parse_aspect = self.fix.parse_aspect
        self.assertEqual(parse_aspect("4:3"), (4, 3))
        self.assertEqual(parse_aspect("16:9"), (16, 9))
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_aspect("1:1")

    def test_calc_display_width_rounds_and_even(self):
        calc_display_width = self.fix.calc_display_width
        self.assertEqual(calc_display_width(576, (16, 9)), 1024)
        self.assertEqual(calc_display_width(576, (4, 3)), 768)
        self.assertEqual(calc_display_width(577, (4, 3)), 770)

    def test_iter_mkvs(self):
        iter_mkvs = self.fix.iter_mkvs
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "one.mkv").write_bytes(b"data")
            (base / "two.txt").write_bytes(b"data")
            sub = base / "sub"
            sub.mkdir()
            (sub / "three.mkv").write_bytes(b"data")

            non_recursive = list(iter_mkvs([base], recursive=False))
            self.assertEqual(non_recursive, [base / "one.mkv"])

            recursive = set(iter_mkvs([base], recursive=True))
            self.assertEqual(recursive, {base / "one.mkv", sub / "three.mkv"})

            single = list(iter_mkvs([sub / "three.mkv"], recursive=False))
            self.assertEqual(single, [sub / "three.mkv"])

    def test_apply_aspect_skips_if_already_matching(self):
        apply_aspect = self.fix.apply_aspect
        with mock.patch.object(self.fix, "probe_video") as probe_video:
            probe_video.return_value = (720, 576, "16:9", "64:45")
            with mock.patch.object(self.fix.subprocess, "run") as run:
                changed = apply_aspect(Path("dummy.mkv"), (16, 9), dry_run=False)
        self.assertFalse(changed)
        run.assert_not_called()

    def test_apply_aspect_updates_when_mismatch(self):
        apply_aspect = self.fix.apply_aspect
        with mock.patch.object(self.fix, "probe_video") as probe_video:
            probe_video.return_value = (720, 576, "4:3", "16:15")
            with mock.patch.object(self.fix.subprocess, "run") as run:
                changed = apply_aspect(Path("dummy.mkv"), (16, 9), dry_run=False)
        self.assertTrue(changed)
        run.assert_called_once()
        cmd = run.call_args[0][0]
        self.assertIn("mkvpropedit", cmd[0])
        self.assertIn("display-width=1024", cmd)
        self.assertIn("display-height=576", cmd)

    def test_apply_aspect_dry_run(self):
        apply_aspect = self.fix.apply_aspect
        with mock.patch.object(self.fix, "probe_video") as probe_video:
            probe_video.return_value = (720, 576, "4:3", "16:15")
            with mock.patch.object(self.fix.subprocess, "run") as run:
                changed = apply_aspect(Path("dummy.mkv"), (16, 9), dry_run=True)
        self.assertTrue(changed)
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
