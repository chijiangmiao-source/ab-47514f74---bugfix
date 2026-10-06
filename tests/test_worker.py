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

    def test_distinct_export_ids_equivalent_content_get_independent_artifacts(self):
        """Different export ids, business-identical records + same frozen rules:
        each export renders, freezes and publishes its OWN artifact."""
        records = [{"ts": "t0", "lat": 31.2, "depth_m": 10, "vessel_id": "V-1"}]
        _, receipt_a = store.submit_export(self.conn, "E-A", records)
        self.process_published("E-A")
        _, receipt_b = store.submit_export(self.conn, "E-B", records)  # unchanged rules
        self.process_published("E-B")

        row_a = store.get_export(self.conn, "E-A")
        row_b = store.get_export(self.conn, "E-B")
        self.assertEqual("PUBLISHED", row_a["stage"])
        self.assertEqual("PUBLISHED", row_b["stage"])
        # independent receipts, freeze times, files and digests
        self.assertNotEqual(receipt_a["receipt_id"], receipt_b["receipt_id"])
        self.assertNotEqual(receipt_a["received_at"], receipt_b["received_at"])
        self.assertNotEqual(row_a["artifact_digest"], row_b["artifact_digest"])
        self.assertTrue(row_a["artifact_path"].endswith("E-A.json"))
        self.assertTrue(row_b["artifact_path"].endswith("E-B.json"))
        self.assertEqual(2, len(artifacts.list_published_files()))
        for row, export_id in ((row_a, "E-A"), (row_b, "E-B")):
            doc = json.loads(artifacts.load_verified(row))
            self.assertEqual(export_id, doc["export_id"])
            self.assertEqual(row["received_at"], doc["received_at"])
            self.assertEqual(1, len(store.published_artifacts(self.conn, export_id)))

    def process_published(self, export_id):
        fencing = store.acquire_lease(self.conn, worker.lease_resource(export_id), "w-test", 5)
        self.assertEqual("published", worker.process_export(self.conn, export_id, "w-test", fencing))

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
