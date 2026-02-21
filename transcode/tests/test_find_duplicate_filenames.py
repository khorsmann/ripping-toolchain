import importlib.util
import tempfile
import unittest
from pathlib import Path


def load_module():
    module_path = Path(__file__).resolve().parents[1] / "find_duplicate_filenames.py"
    spec = importlib.util.spec_from_file_location(
        "find_duplicate_filenames_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FindDuplicateFilenamesTests(unittest.TestCase):
    def test_detects_duplicates_by_filename(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "a").mkdir()
            (base / "b").mkdir()
            (base / "a" / "episode.mkv").touch()
            (base / "b" / "episode.mkv").touch()
            (base / "b" / "other.mkv").touch()

            result = mod.find_duplicate_filenames(base, "*.mkv", True, False)

            self.assertIn("episode.mkv", result)
            self.assertEqual(len(result["episode.mkv"]), 2)
            self.assertNotIn("other.mkv", result)

    def test_case_sensitive_glob_matching(self):
        mod = load_module()
        self.assertTrue(mod.matches_glob("EPISODE.MKV", "*.mkv", True))
        self.assertFalse(mod.matches_glob("EPISODE.MKV", "*.mkv", False))

    def test_stem_mode_groups_across_extensions(self):
        mod = load_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "a").mkdir()
            (base / "b").mkdir()
            (base / "a" / "movie.mkv").touch()
            (base / "b" / "movie.mp4").touch()

            result = mod.find_duplicate_filenames(base, "*", True, True)

            self.assertIn("movie", result)
            self.assertEqual(len(result["movie"]), 2)


if __name__ == "__main__":
    unittest.main()
