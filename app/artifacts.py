"""Artifact filesystem operations: temp write, digest verify, atomic publish.

Publish uses hard-link + unlink on the same filesystem: atomic, and it never
clobbers an already-published artifact (second publisher gets FileExistsError
and must verify the existing digest instead).
"""
import hashlib
import json
import os
import time
import uuid

from . import config


class PublishedMismatch(Exception):
    """A different artifact already occupies the published path."""


class ArtifactMissing(Exception):
    pass


class DigestMismatch(Exception):
    pass


class IdentityMismatch(Exception):
    """Artifact bytes embed a different frozen identity than the export row."""


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def tmp_path(export_id, token):
    return os.path.join(config.tmp_dir(), "%s.%s.part" % (export_id, token))


def published_path(export_id):
    return os.path.join(config.published_dir(), "%s.json" % export_id)


def write_tmp(path, data):
    with open(path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish(tmp, dst, expected_digest):
    """Atomically publish tmp at dst without clobbering. Returns 'linked' or 'dedup'."""
    if os.path.exists(dst):
        if sha256_file(dst) == expected_digest:
            if os.path.exists(tmp):
                os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    try:
        os.link(tmp, dst)
    except FileExistsError:
        if sha256_file(dst) == expected_digest:
            os.unlink(tmp)
            return "dedup"
        raise PublishedMismatch("published path holds different content: %s" % dst)
    os.unlink(tmp)
    _fsync_dir(os.path.dirname(dst))
    return "linked"


def quarantine(path):
    os.makedirs(config.quarantine_dir(), exist_ok=True)
    target = os.path.join(
        config.quarantine_dir(),
        "%s.%d" % (os.path.basename(path), int(time.time() * 1000)),
    )
    os.replace(path, target)
    return target


def tmp_files_for(export_id):
    prefix = export_id + "."
    out = []
    directory = config.tmp_dir()
    if os.path.isdir(directory):
        for name in os.listdir(directory):
            if name.startswith(prefix) and name.endswith(".part"):
                out.append(os.path.join(directory, name))
    return out


def cleanup_tmp_for(export_id):
    removed = []
    for path in tmp_files_for(export_id):
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    return removed


def list_tmp_files():
    directory = config.tmp_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".part")]


def list_published_files():
    directory = config.published_dir()
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory) if name.endswith(".json")]


def load_verified(export_row):
    """Read a published artifact only when it is the export's OWN frozen one.

    Two independent checks guard the download path, so unverified or another
    export id's content can never be served even if a stale/aliased DB row
    survives a restart:

      1. integrity: file bytes hash to the recorded ``artifact_digest``;
      2. identity: the embedded freeze fields (export id, first-receipt time,
         input/rules digests) match this row exactly.
    """
    path = export_row.get("artifact_path")
    if not path or not os.path.exists(path):
        raise ArtifactMissing("artifact file is missing")
    with open(path, "rb") as fh:
        data = fh.read()
    if sha256_bytes(data) != export_row.get("artifact_digest"):
        raise DigestMismatch("artifact digest mismatch")
    try:
        embedded = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise IdentityMismatch("artifact is not a parseable export document")
    for field in ("export_id", "received_at", "input_digest", "rules_digest"):
        if embedded.get(field) != export_row.get(field):
            raise IdentityMismatch(
                "artifact embeds a different frozen %s than export %s"
                % (field, export_row.get("export_id"))
            )
    return data


def materialize_own(export_id, data, digest):
    """Ensure ``published/<export_id>.json`` holds exactly this export's bytes.

    Used to converge a legacy aliased PUBLISHED row onto its own artifact: the
    bytes are rendered from the export's frozen record, written to a temp file,
    re-read for a digest check, then atomically linked to the canonical path.
    A foreign file occupying that path is quarantined rather than trusted; a
    same-content file is reused. Returns a small ``via`` descriptor.
    """
    dst = published_path(export_id)
    if os.path.exists(dst):
        if sha256_file(dst) == digest:
            return "already_own"
        quarantine(dst)  # foreign/corrupt content at our canonical path
    tmp = tmp_path(export_id, uuid.uuid4().hex[:8])
    write_tmp(tmp, data)
    if sha256_file(tmp) != digest:
        quarantine(tmp)
        raise DigestMismatch("recomputed own artifact failed digest verification")
    try:
        return publish(tmp, dst, digest)  # 'linked' (or 'dedup' on a racing peer)
    except PublishedMismatch:
        if os.path.exists(dst) and sha256_file(dst) == digest:
            return "dedup"
        raise
