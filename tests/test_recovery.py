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

    def test_load_verified_rejects_another_export_ids_bytes(self):
        """Even with a matching recorded digest, another identifier's frozen
        artifact must never be downloadable for this export."""
        store.submit_export(self.conn, "E-2", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        row2 = store.get_export(self.conn, "E-2")
        data2 = render_artifact_bytes(row2)
        digest2 = hashlib.sha256(data2).hexdigest()
        with open(artifacts.published_path("E-2"), "wb") as fh:  # E-2's own file on disk
            fh.write(data2)
        # E-1 row aliases E-2's artifact (the old cross-decision reuse bug).
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", digest2, artifacts.published_path("E-2"), "t", "unit")
        with self.assertRaises(artifacts.IdentityMismatch):
            artifacts.load_verified(self.export())


class LegacyAliasConvergenceTest(RecoveryTestBase):
    def _publish_properly(self, export_id):
        row = store.get_export(self.conn, export_id)
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        path = artifacts.published_path(export_id)
        with open(path, "wb") as fh:
            fh.write(data)
        with store.immediate(self.conn):
            store.record_artifact(self.conn, export_id, "published", path, digest)
            store.mark_published(self.conn, export_id, digest, path, "old", "linked")
        return row, digest

    def _alias(self, export_id, target_path, target_digest):
        with store.immediate(self.conn):
            store.record_artifact(self.conn, export_id, "published", target_path, target_digest)
            store.mark_published(self.conn, export_id, target_digest, target_path,
                                 "old", "matching_decision_reuse")

    def test_aliased_published_export_converges_to_own_artifact(self):
        records = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        store.submit_export(self.conn, "E-2", records)  # E-1 submitted by base class, same content
        row1, digest1 = self._publish_properly("E-1")
        # pre-fix behavior: E-2 published onto E-1's file/digest.
        self._alias("E-2", artifacts.published_path("E-1"), digest1)
        aliased = store.get_export(self.conn, "E-2")
        own_digest = hashlib.sha256(render_artifact_bytes(aliased)).hexdigest()
        self.assertNotEqual(digest1, own_digest)  # freeze time/id differ
        with self.assertRaises(artifacts.IdentityMismatch):
            artifacts.load_verified(aliased)

        repaired = recovery.converge_legacy_aliases(self.conn, "repair")
        self.assertEqual(["E-2"], repaired)

        fixed = store.get_export(self.conn, "E-2")
        self.assertEqual("PUBLISHED", fixed["stage"])  # terminal stage never regressed
        self.assertEqual(own_digest, fixed["artifact_digest"])
        self.assertTrue(fixed["artifact_path"].endswith("E-2.json"))
        doc = json.loads(artifacts.load_verified(fixed))
        self.assertEqual("E-2", doc["export_id"])
        self.assertEqual(fixed["received_at"], doc["received_at"])
        # independent files, first export untouched, evidence retained
        self.assertEqual(2, len(artifacts.list_published_files()))
        self.assertEqual(digest1, store.get_export(self.conn, "E-1")["artifact_digest"])
        kinds = {a["kind"] for a in self.conn.execute(
            "SELECT kind FROM artifacts WHERE export_id = 'E-2'").fetchall()}
        self.assertIn("aborted", kinds)
        events = [e["event"] for e in store.export_events(self.conn, "E-2")]
        self.assertIn("artifact_converged", events)

    def test_convergence_is_idempotent(self):
        row1, digest1 = self._publish_properly("E-1")
        self.assertEqual([], recovery.converge_legacy_aliases(self.conn, "repair"))
        self.assertEqual([], recovery.converge_legacy_aliases(self.conn, "repair"))
        self.assertEqual(digest1, store.get_export(self.conn, "E-1")["artifact_digest"])

    def test_convergence_survives_alias_restart_scenario(self):
        """The repair result must itself be stable across another pass (restart)."""
        records = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        store.submit_export(self.conn, "E-2", records)
        _, digest1 = self._publish_properly("E-1")
        self._alias("E-2", artifacts.published_path("E-1"), digest1)
        recovery.converge_legacy_aliases(self.conn, "repair-1")
        first = store.get_export(self.conn, "E-2")
        recovery.converge_legacy_aliases(self.conn, "repair-2")  # simulate restart
        second = store.get_export(self.conn, "E-2")
        self.assertEqual(first["artifact_digest"], second["artifact_digest"])
        self.assertEqual(first["artifact_path"], second["artifact_path"])
        artifacts.load_verified(second)  # still verifiable as its own


if __name__ == "__main__":
    unittest.main()
