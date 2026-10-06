"""Crash recovery.

Runs under the export's lease (worker startup and every tick). For each
unfinished export, consult the journal/artifact records plus on-disk digests:

* the staged temp artifact is complete (digest matches the recorded and the
  deterministically recomputed digest) -> converge: publish that very artifact;
* a published file already exists with the expected digest (crash between the
  atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, missing file, orphans) ->
  clean up the残缺 artifacts and requeue the export.

Legacy convergence: earlier versions reused the FIRST published export's file
for a business-equivalent submission under a DIFFERENT export id. Those
PUBLISHED rows alias another export's bytes. They are repaired in place (stage
stays terminal) by materializing each export's own artifact from its frozen
record and repointing the row, so the download API can never serve another
identifier's content.
"""
import hashlib
import os

from . import artifacts, store
from .render import render_artifact_bytes


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _owns_verified_file(export_row):
    """True iff the row points at its own canonical file that passes checks."""
    own_path = os.path.abspath(artifacts.published_path(export_row["export_id"]))
    path = export_row.get("artifact_path")
    if not path or os.path.abspath(path) != own_path:
        return False
    try:
        artifacts.load_verified(export_row)
    except (artifacts.ArtifactMissing, artifacts.DigestMismatch, artifacts.IdentityMismatch):
        return False
    return True


def converge_legacy_aliases(conn, actor):
    """Repair PUBLISHED rows aliasing another export's artifact.

    Each affected export is converged to its own independently verifiable file
    rendered from its frozen record (records + rules snapshot + first receipt).
    The PUBLISHED stage is never touched, so publication never regresses; the
    stale artifact row is aborted as evidence. Idempotent and concurrency-safe:
    a second pass finds the row already converged and makes no change.
    """
    repaired = []
    for export in store.all_published_exports(conn):
        if _owns_verified_file(export):
            continue
        export_id = export["export_id"]
        data, digest = _expected(export)
        own_path = artifacts.published_path(export_id)
        via = artifacts.materialize_own(export_id, data, digest)
        if store.repoint_published_artifact(conn, export_id, digest, own_path, actor,
                                            "legacy_alias_repair via=%s" % via):
            repaired.append(export_id)
    return repaired


def _converge(conn, export_id, digest, actor, via):
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), actor, via)


def recover_export(conn, export_id, actor):
    """Recover one export. Caller must hold the export's lease."""
    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    _, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        if artifacts.sha256_file(pub) == expected_digest:
            _converge(conn, export_id, expected_digest, actor, "recovery_published_file")
            artifacts.cleanup_tmp_for(export_id)
            return "converged"
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_quarantined_published", target)

    # Case 2: a staged temp artifact whose digest matches journal + recompute.
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if (
            os.path.exists(path)
            and artifacts.sha256_file(path) == row["digest"] == expected_digest
        ):
            artifacts.publish(path, pub, row["digest"])
            _converge(conn, export_id, row["digest"], actor, "recovery_staged_artifact")
            artifacts.cleanup_tmp_for(export_id)
            return "converged"

    # Case 3: incomplete/mismatched remains -> clean up and requeue.
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
        store.journal(conn, export_id, actor, "recovery_cleanup", "removed=%d" % len(removed))
        store.requeue(conn, export_id, actor, "recovery_cleanup removed=%d" % len(removed))
    return "requeued"


def sweep_orphans(conn, actor, older_than_seconds=30.0):
    """Delete temp files not referenced by any staged artifact record."""
    import time

    removed = []
    known = set()
    for export in store.list_exports(conn, limit=1000):
        for row in store.staged_artifacts(conn, export["export_id"]):
            known.add(os.path.abspath(row["path"]))
    now = time.time()
    for path in artifacts.list_tmp_files():
        if os.path.abspath(path) in known:
            continue
        if now - os.path.getmtime(path) < older_than_seconds:
            continue  # may belong to an in-flight staging; leave it alone
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    if removed:
        with store.immediate(conn):
            store.journal(conn, None, actor, "recovery_orphan_sweep", "removed=%d" % len(removed))
    return removed
