import os
import tempfile
import unittest
from pathlib import Path

from misc.rename_eps import collect_renames


class CollectRenamesTests(unittest.TestCase):
    def test_collect_renames_orders_descending_for_positive_offset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            for ep in (1, 2, 10):
                (base / f"S01E{ep:02d}.mkv").touch()

            renames = collect_renames(base, offset=1)

            episodes = [ep for ep, _, _ in renames]
            self.assertEqual(episodes, [10, 2, 1])

    def test_downward_renames_do_not_overwrite_existing_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            ep1 = base / "S01E01.mkv"
            ep2 = base / "S01E02.mkv"

            ep1.write_text("episode one", encoding="utf-8")
            ep2.write_text("episode two", encoding="utf-8")

            renames = collect_renames(base, offset=-1)
            for _, src, dst in renames:
                os.rename(src, dst)

            self.assertTrue((base / "S01E00.mkv").exists())
            self.assertTrue((base / "S01E01.mkv").exists())
            self.assertEqual(
                (base / "S01E00.mkv").read_text(encoding="utf-8"), "episode one"
            )
            self.assertEqual(
                (base / "S01E01.mkv").read_text(encoding="utf-8"), "episode two"
            )

    def test_makemkv_pattern(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "SHOW-S02E_t00.mkv").touch()
            (base / "SHOW-S02E_t01.mkv").touch()

            renames = collect_renames(base, offset=1)

            targets = [dst.name for _, _, dst in renames]
            self.assertEqual(
                sorted(targets),
                ["SHOW-S02E01.mkv", "SHOW-S02E02.mkv"],
            )


if __name__ == "__main__":
    unittest.main()
