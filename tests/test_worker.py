import hashlib
import json
import os
import tempfile
import threading
import time
import unittest

from app import artifacts, config, store, worker
from app.render import render_artifact_bytes


class WorkerTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)


class ProcessTest(WorkerTestBase):
    def test_process_publishes_verified_artifact(self):
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        fencing = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-test", 5)
        result = worker.process_export(self.conn, "E-1", "w-test", fencing)
        self.assertEqual("published", result)
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        data = artifacts.load_verified(row)  # digest verified
        self.assertEqual(hashlib.sha256(data).hexdigest(), row["artifact_digest"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_two_workers_publish_exactly_once(self):
        """Two racing worker loops: one export, one published artifact, no regression."""
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        stop = threading.Event()

        def loop(name):
            conn = store.connect()
            try:
                while not stop.is_set():
                    try:
                        worker.tick(conn, name)
                    except Exception:
                        conn.rollback()
                    time.sleep(0.02)
            finally:
                conn.close()

        threads = [threading.Thread(target=loop, args=("w-%d" % i,)) for i in range(2)]
        for t in threads:
            t.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            row = store.get_export(self.conn, "E-1")
            if row["stage"] == "PUBLISHED":
                break
            time.sleep(0.05)
        time.sleep(0.5)  # give the loser a chance to misbehave
        stop.set()
        for t in threads:
            t.join()

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))
        self.assertEqual(1, len(artifacts.list_published_files()))
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])

    def test_equivalent_exports_publish_independent_artifacts(self):
        """Same business content under two export ids: each publishes its own
        artifact (own export_id + frozen receipt), never a shared file."""
        recs = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        store.submit_export(self.conn, "E-1", recs)
        store.submit_export(self.conn, "E-2", [dict(reversed(list(recs[0].items())))])
        for export_id in ("E-1", "E-2"):
            fencing = store.acquire_lease(self.conn, worker.lease_resource(export_id), "w-test", 5)
            self.assertEqual("published", worker.process_export(self.conn, export_id, "w-test", fencing))

        row1 = store.get_export(self.conn, "E-1")
        row2 = store.get_export(self.conn, "E-2")
        self.assertNotEqual(row1["artifact_digest"], row2["artifact_digest"])
        self.assertNotEqual(row1["artifact_path"], row2["artifact_path"])
        self.assertEqual(2, len(artifacts.list_published_files()))
        for export_id, row in (("E-1", row1), ("E-2", row2)):
            data = artifacts.load_verified(row)
            self.assertEqual(hashlib.sha256(data).hexdigest(), row["artifact_digest"])
            doc = json.loads(data)
            self.assertEqual(export_id, doc["export_id"])
            self.assertEqual(row["received_at"], doc["received_at"])
        # same masking rules + same business records -> same masked payload
        self.assertEqual(json.loads(artifacts.load_verified(row1))["records"],
                         json.loads(artifacts.load_verified(row2))["records"])

    def test_tick_recovers_crashed_export_after_lease_expiry(self):
        """Simulate a crashed worker: staged artifact + expired lease -> tick converges."""
        os.environ["LEASE_TTL_SECONDS"] = "0.05"
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        row = store.get_export(self.conn, "E-1")
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "dead")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))
        # dead worker's lease, already expired
        store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-dead", 0.01)
        time.sleep(0.06)

        worker.tick(self.conn, "w-alive")

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("published", events)


if __name__ == "__main__":
    unittest.main()
