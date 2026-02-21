import importlib.util
import os
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock


def install_fake_paho():
    paho_mod = types.ModuleType("paho")
    mqtt_pkg = types.ModuleType("paho.mqtt")
    client_mod = types.ModuleType("paho.mqtt.client")

    class CallbackAPIVersion:
        VERSION2 = object()

    class Client:
        def __init__(self, **_kwargs):
            pass

        def username_pw_set(self, *_args, **_kwargs):
            pass

        def tls_set(self, *_args, **_kwargs):
            pass

        def publish(self, *_args, **_kwargs):
            pass

        def user_data_set(self, *_args, **_kwargs):
            pass

        def connect(self, *_args, **_kwargs):
            pass

        def subscribe(self, *_args, **_kwargs):
            pass

        def loop_forever(self):
            pass

    client_mod.CallbackAPIVersion = CallbackAPIVersion
    client_mod.Client = Client
    mqtt_pkg.client = client_mod
    paho_mod.mqtt = mqtt_pkg
    return {
        "paho": paho_mod,
        "paho.mqtt": mqtt_pkg,
        "paho.mqtt.client": client_mod,
    }


def load_transcode_mqtt_module(tmpdir: Path):
    module_path = Path(__file__).resolve().parents[1] / "transcode_mqtt.py"
    spec = importlib.util.spec_from_file_location("transcode_mqtt_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader

    env = {
        "MQTT_HOST": "localhost",
        "MQTT_USER": "user",
        "MQTT_PASSWORD": "pass",
        "SRC_BASE": str(tmpdir),
        "SERIES_DST_BASE": str(tmpdir / "series"),
        "MOVIE_DST_BASE": str(tmpdir / "movies"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        spec.loader.exec_module(module)
    return module


class SQLiteQueueTests(unittest.TestCase):
    def test_sqlite_queue_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            fake_modules = install_fake_paho()
            with mock.patch.dict(sys.modules, fake_modules):
                mod = load_transcode_mqtt_module(tmp)

            queue = mod.SQLiteJobQueue(tmp / "jobs.sqlite3", poll_interval=0.1)
            queue.put({"path": "/tmp/input", "mode": "series"})

            job = queue.get()
            self.assertEqual(job["path"], "/tmp/input")
            self.assertIn("_queue_id", job)

            queue.task_done(job)
            remaining = queue.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            self.assertEqual(remaining, 0)

    def test_sqlite_queue_reclaims_stale_claim(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            fake_modules = install_fake_paho()
            with mock.patch.dict(sys.modules, fake_modules):
                mod = load_transcode_mqtt_module(tmp)

            queue = mod.SQLiteJobQueue(
                tmp / "jobs.sqlite3", poll_interval=0.1, claim_ttl_seconds=1
            )
            queue.put({"path": "/tmp/reclaim", "mode": "movie"})
            first = queue.get()
            job_id = first["_queue_id"]

            stale_ts = int(time.time()) - 5
            queue.conn.execute(
                "UPDATE jobs SET claimed_ts = ? WHERE id = ?", (stale_ts, job_id)
            )
            queue.conn.commit()

            reclaimed = queue.get()
            self.assertEqual(reclaimed["_queue_id"], job_id)
            self.assertEqual(reclaimed["path"], "/tmp/reclaim")
            queue.task_done(reclaimed)


if __name__ == "__main__":
    unittest.main()
