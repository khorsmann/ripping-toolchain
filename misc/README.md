# Misc Utilities

## rename_eps.py
Renames episode files whose names contain `SxxExx`. Offsets episode numbers up or down and orders renames to avoid overwriting existing files.

- Dry-run (no changes): `python misc/rename_eps.py /path/to/series`
- Apply offset +1: `python misc/rename_eps.py /path/to/series --apply`
- Apply offset -1: `python misc/rename_eps.py /path/to/series --down --apply`

### Tests
Run the unit tests for the renaming logic from the repo root:

```bash
python -m unittest discover -s misc -p 'test_*.py'
```
