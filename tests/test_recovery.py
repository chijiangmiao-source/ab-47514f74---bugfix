import hashlib
import json
import os
import tempfile
import unittest

from app import artifacts, config, recovery, store
from app.render import render_artifact_bytes


class RecoveryTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def export(self):
        return store.get_export(self.conn, "E-1")

    def expected_digest(self):
        return hashlib.sha256(render_artifact_bytes(self.export())).hexdigest()


class ConvergeTest(RecoveryTestBase):
    def test_complete_staged_artifact_is_published_as_is(self):
        """Crash after staging: recovery converges to the same complete artifact."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "deadbeef")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        # the very same bytes were published, not a regenerated copy
        with open(artifacts.published_path("E-1"), "rb") as fh:
            self.assertEqual(data, fh.read())
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_published_file_present_but_db_not_updated(self):
        """Crash between atomic link and DB update: converge bookkeeping."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "cafe")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path("E-1"), digest)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))


class CleanupTest(RecoveryTestBase):
    def test_partial_write_is_cleaned_and_requeued(self):
        """Crash mid-write: partial temp file removed, export back to RECEIVED."""
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "half")
        artifacts.write_tmp(tmp, data[: len(data) // 2])
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        row = self.export()
        self.assertEqual("RECEIVED", row["stage"])
        self.assertEqual(1, row["attempts"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_corrupted_staged_artifact_is_aborted(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "badc0de")
        artifacts.write_tmp(tmp, data + b"corruption")
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        self.assertEqual([], artifacts.tmp_files_for("E-1"))
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assertEqual("RECEIVED", self.export()["stage"])

    def test_published_export_is_never_touched(self):
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", "d" * 64, "/nowhere", "test", "unit")
        self.assertEqual("none", recovery.recover_export(self.conn, "E-1", "test"))
        self.assertEqual("PUBLISHED", self.export()["stage"])
        self.assertEqual("d" * 64, self.export()["artifact_digest"])

    def test_orphan_sweep_removes_unreferenced_old_temp_files(self):
        orphan = artifacts.tmp_path("E-9", "orphan")
        artifacts.write_tmp(orphan, b"leftover")
        old = 1_600_000_000
        os.utime(orphan, (old, old))
        removed = recovery.sweep_orphans(self.conn, "test", older_than_seconds=1)
        self.assertIn(orphan, removed)
        self.assertFalse(os.path.exists(orphan))


class PublishRepairTest(RecoveryTestBase):
    """Legacy rows: PUBLISHED but pointing at another export's artifact (the
    old matching-decision reuse). The startup sweep must converge them onto
    their own verifiable artifact without regressing the stage."""

    def _publish_normally(self, export_id):
        row = store.get_export(self.conn, export_id)
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path(export_id, "seed")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path(export_id), digest)
        with store.immediate(self.conn):
            store.record_artifact(self.conn, export_id, "published",
                                  artifacts.published_path(export_id), digest)
            store.mark_published(self.conn, export_id, digest,
                                 artifacts.published_path(export_id), "test", "unit")
        return store.get_export(self.conn, export_id)

    def _make_legacy_reuse(self, source_row, target_id="E-2"):
        """Reproduce the historical bug: target marked PUBLISHED with the
        source export's digest/path (shared artifact)."""
        store.submit_export(self.conn, target_id,
                            [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        with store.immediate(self.conn):
            store.record_artifact(self.conn, target_id, "published",
                                  source_row["artifact_path"], source_row["artifact_digest"])
            store.mark_published(self.conn, target_id, source_row["artifact_digest"],
                                 source_row["artifact_path"], "legacy", "matching_decision_reuse")
        return store.get_export(self.conn, target_id)

    def test_download_refuses_foreign_artifact(self):
        source = self._publish_normally("E-1")
        legacy = self._make_legacy_reuse(source)
        self.assertEqual("PUBLISHED", legacy["stage"])
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(legacy)  # must not expose E-1's content
        # the source export itself still downloads fine
        self.assertEqual(source["artifact_digest"],
                         hashlib.sha256(artifacts.load_verified(source)).hexdigest())

    def test_sweep_converges_legacy_row_to_own_artifact(self):
        source = self._publish_normally("E-1")
        legacy = self._make_legacy_reuse(source)

        repaired = recovery.repair_published_exports(self.conn, "test")

        self.assertEqual(["E-2"], repaired)
        row = store.get_export(self.conn, "E-2")
        self.assertEqual("PUBLISHED", row["stage"])  # no regression
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])
        self.assertNotEqual(source["artifact_digest"], row["artifact_digest"])
        self.assertEqual(artifacts.published_path("E-2"), row["artifact_path"])
        # own file on disk, serving E-2's own identity
        with open(artifacts.published_path("E-2"), "rb") as fh:
            self.assertEqual(expected, hashlib.sha256(fh.read()).hexdigest())
        doc = json.loads(artifacts.load_verified(row))
        self.assertEqual("E-2", doc["export_id"])
        self.assertEqual(row["received_at"], doc["received_at"])
        # artifacts table converged: exactly one published row, pointing at own file
        pubs = store.published_artifacts(self.conn, "E-2")
        self.assertEqual(1, len(pubs))
        self.assertEqual(row["artifact_path"], pubs[0]["path"])
        self.assertEqual(row["artifact_digest"], pubs[0]["digest"])
        # source export untouched
        self.assertEqual(source["artifact_digest"],
                         store.get_export(self.conn, "E-1")["artifact_digest"])
        events = [e["event"] for e in store.export_events(self.conn, "E-2")]
        self.assertIn("publish_repaired", events)

    def test_sweep_is_idempotent_and_leaves_healthy_rows_alone(self):
        source = self._publish_normally("E-1")
        self._make_legacy_reuse(source)
        recovery.repair_published_exports(self.conn, "test")
        snapshot = {eid: store.get_export(self.conn, eid) for eid in ("E-1", "E-2")}
        self.assertEqual([], recovery.repair_published_exports(self.conn, "test"))
        for eid, before in snapshot.items():
            after = store.get_export(self.conn, eid)
            self.assertEqual(before["artifact_digest"], after["artifact_digest"])
            self.assertEqual(before["artifact_path"], after["artifact_path"])

    def test_sweep_restores_missing_published_file(self):
        row = self._publish_normally("E-1")
        os.unlink(artifacts.published_path("E-1"))
        repaired = recovery.repair_published_exports(self.conn, "test")
        self.assertEqual(["E-1"], repaired)
        after = store.get_export(self.conn, "E-1")
        self.assertEqual(row["artifact_digest"], after["artifact_digest"])
        self.assertEqual(row["artifact_digest"],
                         hashlib.sha256(artifacts.load_verified(after)).hexdigest())

    def test_repair_guard_requires_published_stage(self):
        self.assertFalse(store.converge_published_artifact(
            self.conn, "E-1", "d" * 64, "/p", "test"))  # E-1 is RECEIVED
        self.assertIsNone(store.get_export(self.conn, "E-1")["artifact_digest"])


class PublishPrimitiveTest(RecoveryTestBase):
    def test_publish_never_clobbers(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"same-content")
        artifacts.write_tmp(b, b"same-content")
        dst = artifacts.published_path("E-1")
        digest = artifacts.sha256_bytes(b"same-content")
        self.assertEqual("linked", artifacts.publish(a, dst, digest))
        self.assertEqual("dedup", artifacts.publish(b, dst, digest))
        self.assertFalse(os.path.exists(a))
        self.assertFalse(os.path.exists(b))
        self.assertEqual(1, len(artifacts.list_published_files()))

    def test_publish_refuses_different_content(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"content-a")
        artifacts.write_tmp(b, b"content-b")
        dst = artifacts.published_path("E-1")
        artifacts.publish(a, dst, artifacts.sha256_bytes(b"content-a"))
        with self.assertRaises(artifacts.PublishedMismatch):
            artifacts.publish(b, dst, artifacts.sha256_bytes(b"content-b"))

    def test_load_verified_rejects_tampered_bytes(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", digest, artifacts.published_path("E-1"), "t", "unit")
        artifacts.write_tmp(artifacts.published_path("E-1"), data + b"tamper")
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(self.export())


if __name__ == "__main__":
    unittest.main()
