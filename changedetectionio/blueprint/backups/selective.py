"""
Selective backup restore: preview → confirm → apply.

Unlike the bulk restore in restore.py, this flow never touches the live datastore
before explicit confirmation:

1. analyze  - the uploaded zip is extracted into a staging area below the datastore
              path and a deterministic add/update/skip plan is written to plan.json.
2. preview  - the plan is rendered as a checklist (skips are shown with reasons and
              cannot be selected).
3. apply    - only selected entities are published, tags first then watches, each in
              its own datastore.lock transaction with atomic file writes. A journal
              makes the run resumable: re-running against the same backup converges
              to the same state because classification is a pure function of
              (backup content, current local state).

Staging layout (same filesystem as the datastore so directory publishes are atomic):

    {datastore}/.restore-staging/{token}/
        plan.json
        journal.json
        entities/{uuid}/...
"""

import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import time
import uuid as uuid_builder

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from flask_babel import gettext
from loguru import logger

from changedetectionio.flask_app import login_optionally_required
from changedetectionio.model.schema_utils import (
    SYSTEM_MANAGED_NON_SPEC_FIELDS,
    get_readonly_tag_fields,
    get_readonly_watch_fields,
)
from .restore import _MAX_UPLOAD_BYTES, _UUID_RE, bulk_restore_running, safe_extract_zip

STAGING_DIRNAME = ".restore-staging"
# Staging tokens and leftover publish temp dirs older than this are garbage collected.
STAGING_MAX_AGE_SECONDS = 24 * 60 * 60
PUBLISH_TMP_PREFIX = ".restore-tmp-"

ACTION_ADD = "add"
ACTION_UPDATE = "update"
ACTION_SKIP = "skip"

REASON_LOCAL_MODIFIED = "local_modified"
REASON_URL_CONFLICT = "url_conflict"
REASON_TITLE_CONFLICT = "title_conflict"
REASON_INVALID_ENTRY = "invalid_backup_entry"

_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


# ---------------------------------------------------------------------------
# Staging area
# ---------------------------------------------------------------------------

def _staging_root(datastore):
    return os.path.join(datastore.datastore_path, STAGING_DIRNAME)


def _staging_dir(datastore, token):
    return os.path.join(_staging_root(datastore), token)


def _entities_dir(datastore, token):
    return os.path.join(_staging_dir(datastore, token), "entities")


def _plan_path(datastore, token):
    return os.path.join(_staging_dir(datastore, token), "plan.json")


def _journal_path(datastore, token):
    return os.path.join(_staging_dir(datastore, token), "journal.json")


def _write_json_atomic(path, payload):
    fd, tmp_path = tempfile.mkstemp(prefix=".json-", suffix=".tmp", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def _read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _prune_stale_files(datastore):
    """Remove staging directories and leftover publish temp dirs older than the TTL."""
    cutoff = time.time() - STAGING_MAX_AGE_SECONDS

    root = _staging_root(datastore)
    if os.path.isdir(root):
        for entry in os.scandir(root):
            if entry.is_dir() and entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry.path, ignore_errors=True)

    try:
        for entry in os.scandir(datastore.datastore_path):
            if entry.is_dir() and entry.name.startswith(PUBLISH_TMP_PREFIX) and entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry.path, ignore_errors=True)
    except FileNotFoundError:
        pass


def validate_token(token):
    if not token or not _TOKEN_RE.match(token):
        abort(404)
    return token


# ---------------------------------------------------------------------------
# Configuration signatures (the "was this entity manually modified?" test)
# ---------------------------------------------------------------------------

def _watch_excluded_fields():
    # Read-only runtime fields + internal processor state + last_viewed (opening a diff is
    # not a configuration edit). Everything else is a user-writable configuration field.
    return set(get_readonly_watch_fields()) | set(SYSTEM_MANAGED_NON_SPEC_FIELDS) | {"last_viewed"}


def _tag_excluded_fields():
    return set(get_readonly_tag_fields()) | set(SYSTEM_MANAGED_NON_SPEC_FIELDS) | {"last_viewed"}


def _rehydrated_signature(datastore, entity_type, uuid, entity_dict):
    """Rehydrate both sides through the real models so defaults/normalization match,
    then keep only the user-writable fields."""
    if entity_type == "watch":
        obj = datastore.rehydrate_entity(uuid, dict(entity_dict))
        excluded = _watch_excluded_fields()
    else:
        from changedetectionio.model import Tag

        data = dict(entity_dict)
        data["uuid"] = uuid
        data["processor"] = "restock_diff"
        obj = Tag.model(
            datastore_path=datastore.datastore_path,
            __datastore=datastore.data,
            default=data,
        )
        excluded = _tag_excluded_fields()

    return {k: v for k, v in obj.items() if k not in excluded and not str(k).startswith("__")}


def _processor_config_hashes(entity_dir):
    """sha256 of processor-specific config files ({processor}.json) in an entity directory."""
    hashes = {}
    if not entity_dir or not os.path.isdir(entity_dir):
        return hashes
    for entry in os.scandir(entity_dir):
        if entry.is_file() and entry.name.endswith(".json") and entry.name not in ("watch.json", "tag.json"):
            with open(entry.path, "rb") as f:
                hashes[entry.name] = hashlib.sha256(f.read()).hexdigest()
    return dict(sorted(hashes.items()))


def _diff_signatures(backup_sig, local_sig):
    return sorted(k for k in set(backup_sig) | set(local_sig) if backup_sig.get(k) != local_sig.get(k))


def _diff_config_files(backup_dir, local_dir):
    backup_hashes = _processor_config_hashes(backup_dir)
    local_hashes = _processor_config_hashes(local_dir)
    return sorted(k for k in set(backup_hashes) | set(local_hashes) if backup_hashes.get(k) != local_hashes.get(k))


# ---------------------------------------------------------------------------
# Plan analysis
# ---------------------------------------------------------------------------

def _load_entity_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _classify_watch(uuid, entity_dir, datastore, local_url_index):
    watch_json_path = os.path.join(entity_dir, "watch.json")
    try:
        backup_data = _load_entity_json(watch_json_path)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Selective restore: unreadable watch.json for {uuid}: {e}")
        return {
            "entity_type": "watch", "uuid": uuid, "name": uuid,
            "action": ACTION_SKIP, "reason": REASON_INVALID_ENTRY,
            "details": {"error": str(e)}, "selectable": False,
        }

    url = (backup_data.get("url") or "").strip()
    entry = {
        "entity_type": "watch", "uuid": uuid, "name": url or uuid,
        "action": ACTION_SKIP, "reason": "", "details": {}, "selectable": False,
    }

    current = datastore.data["watching"].get(uuid)
    if current is not None:
        changed_fields = _diff_signatures(
            _rehydrated_signature(datastore, "watch", uuid, backup_data),
            _rehydrated_signature(datastore, "watch", uuid, dict(current)),
        )
        changed_files = _diff_config_files(entity_dir, current.data_dir)
        if not changed_fields and not changed_files:
            entry.update(action=ACTION_UPDATE, selectable=True)
        else:
            entry.update(
                action=ACTION_SKIP, reason=REASON_LOCAL_MODIFIED,
                details={"changed_fields": changed_fields, "changed_config_files": changed_files},
            )
        return entry

    conflict_uuid = local_url_index.get(url.lower()) if url else None
    if conflict_uuid:
        entry.update(reason=REASON_URL_CONFLICT, details={"conflict_uuid": conflict_uuid})
        return entry

    entry.update(action=ACTION_ADD, selectable=True)
    return entry


def _classify_tag(uuid, entity_dir, datastore):
    tag_json_path = os.path.join(entity_dir, "tag.json")
    try:
        backup_data = _load_entity_json(tag_json_path)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Selective restore: unreadable tag.json for {uuid}: {e}")
        return {
            "entity_type": "tag", "uuid": uuid, "name": uuid,
            "action": ACTION_SKIP, "reason": REASON_INVALID_ENTRY,
            "details": {"error": str(e)}, "selectable": False,
        }

    title = (backup_data.get("title") or uuid).strip()
    entry = {
        "entity_type": "tag", "uuid": uuid, "name": title,
        "action": ACTION_SKIP, "reason": "", "details": {},
        "selectable": False, "required_by_watches": [],
    }

    current = datastore.data["settings"]["application"].get("tags", {}).get(uuid)
    if current is not None:
        changed_fields = _diff_signatures(
            _rehydrated_signature(datastore, "tag", uuid, backup_data),
            _rehydrated_signature(datastore, "tag", uuid, dict(current)),
        )
        changed_files = _diff_config_files(entity_dir, current.data_dir)
        if not changed_fields and not changed_files:
            entry.update(action=ACTION_UPDATE, selectable=True)
        else:
            entry.update(
                action=ACTION_SKIP, reason=REASON_LOCAL_MODIFIED,
                details={"changed_fields": changed_fields, "changed_config_files": changed_files},
            )
        return entry

    conflict_uuid = datastore.tag_uuid_for_title(title)
    if conflict_uuid:
        entry.update(reason=REASON_TITLE_CONFLICT, details={"conflict_uuid": conflict_uuid})
        return entry

    entry.update(action=ACTION_ADD, selectable=True)
    return entry


def build_plan(datastore, token):
    """Scan the staged entities and classify every one against the live datastore."""
    entities_dir = _entities_dir(datastore, token)

    local_url_index = {}
    for local_uuid, watch in datastore.data["watching"].items():
        local_url = (watch.get("url") or "").strip().lower()
        if local_url and local_url not in local_url_index:
            local_url_index[local_url] = local_uuid

    watch_entries = []
    tag_entries = []
    tag_entries_by_uuid = {}

    for item in sorted(os.scandir(entities_dir), key=lambda e: e.name):
        if not item.is_dir() or not _UUID_RE.match(item.name):
            continue
        uuid = item.name
        if os.path.exists(os.path.join(item.path, "tag.json")):
            tag_entry = _classify_tag(uuid, item.path, datastore)
            tag_entries.append(tag_entry)
            tag_entries_by_uuid[uuid] = tag_entry
        elif os.path.exists(os.path.join(item.path, "watch.json")):
            watch_entries.append(_classify_watch(uuid, item.path, datastore, local_url_index))

    # Tags referenced by restorable watches are auto-included ("required") and preselected.
    for watch_entry in watch_entries:
        if not watch_entry["selectable"]:
            continue
        watch_json_path = os.path.join(entities_dir, watch_entry["uuid"], "watch.json")
        try:
            watch_data = _load_entity_json(watch_json_path)
        except (json.JSONDecodeError, OSError):
            continue
        for tag_uuid in watch_data.get("tags") or []:
            tag_entry = tag_entries_by_uuid.get(tag_uuid)
            if tag_entry is not None and watch_entry["uuid"] not in tag_entry["required_by_watches"]:
                tag_entry["required_by_watches"].append(watch_entry["uuid"])

    plan = {
        "created_at": int(time.time()),
        "watches": watch_entries,
        "tags": tag_entries,
        "counts": _summarize(watch_entries, tag_entries),
    }
    _write_json_atomic(_plan_path(datastore, token), plan)
    return plan


def _summarize(watch_entries, tag_entries):
    def counts(entries):
        out = {ACTION_ADD: 0, ACTION_UPDATE: 0, ACTION_SKIP: 0}
        for entry in entries:
            out[entry["action"]] += 1
        return out

    return {"watches": counts(watch_entries), "tags": counts(tag_entries)}


def stage_backup(zip_bytes, datastore, original_filename):
    """Validate/extract an uploaded backup zip into a fresh staging directory and build the plan.

    Returns the staging token. Raises zipfile.BadZipFile / ValueError on unsafe input.
    """
    import zipfile

    _prune_stale_files(datastore)

    token = uuid_builder.uuid4().hex
    base_dir = _staging_dir(datastore, token)
    entities_dir = _entities_dir(datastore, token)
    os.makedirs(entities_dir, exist_ok=True)

    backup_sha256 = hashlib.sha256(zip_bytes).hexdigest()

    try:
        safe_extract_zip(_bytes_io(zip_bytes), entities_dir)
        plan = build_plan(datastore, token)
    except Exception:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise

    plan["backup"] = {"filename": original_filename, "sha256": backup_sha256}
    _write_json_atomic(_plan_path(datastore, token), plan)
    return token


def _bytes_io(raw):
    import io

    return io.BytesIO(raw)


def load_plan(datastore, token):
    path = _plan_path(datastore, token)
    if not os.path.isfile(path):
        abort(404)
    return _read_json(path)


# ---------------------------------------------------------------------------
# Apply engine
# ---------------------------------------------------------------------------

def _sync_dir(src_dir, dst_dir):
    """Make dst_dir's file set identical to src_dir's using per-file atomic writes.

    Used for updates and interrupted reruns: the destination directory never disappears,
    each file lands via temp+os.replace, and files absent from the backup are removed
    (whole-directory replacement semantics).
    """
    os.makedirs(dst_dir, exist_ok=True)
    src_files = {e.name for e in os.scandir(src_dir) if e.is_file()}

    for name in sorted(src_files):
        src_path = os.path.join(src_dir, name)
        dst_path = os.path.join(dst_dir, name)
        with open(src_path, "rb") as f:
            payload = f.read()
        fd, tmp_path = tempfile.mkstemp(prefix=".sync-", suffix=".tmp", dir=dst_dir)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(payload)
            os.replace(tmp_path, dst_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    for entry in os.scandir(dst_dir):
        if entry.is_file() and entry.name not in src_files and not entry.name.startswith(".sync-"):
            os.unlink(entry.path)


def _publish_entity_dir(src_dir, dst_dir):
    """Publish a staged entity directory onto its destination. Returns 'added' or 'updated'."""
    if os.path.exists(dst_dir):
        _sync_dir(src_dir, dst_dir)
        return "updated"

    parent = os.path.dirname(dst_dir)
    tmp_dir = tempfile.mkdtemp(prefix=PUBLISH_TMP_PREFIX, dir=parent)
    os.rmdir(tmp_dir)
    try:
        shutil.copytree(src_dir, tmp_dir)
        os.replace(tmp_dir, dst_dir)  # atomic on the same filesystem
        return "added"
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def _local_url_index(datastore):
    index = {}
    for local_uuid, watch in datastore.data["watching"].items():
        url = (watch.get("url") or "").strip().lower()
        if url and url not in index:
            index[url] = local_uuid
    return index


def _apply_one_tag(datastore, token, uuid, plan_entry, journal):
    entities_dir = _entities_dir(datastore, token)
    staging_dir = os.path.join(entities_dir, uuid)
    current_tags = datastore.data["settings"]["application"]["tags"]
    from changedetectionio.model import Tag

    try:
        backup_data = _load_entity_json(os.path.join(staging_dir, "tag.json"))
        current = current_tags.get(uuid)
        if current is not None:
            changed_fields = _diff_signatures(
                _rehydrated_signature(datastore, "tag", uuid, backup_data),
                _rehydrated_signature(datastore, "tag", uuid, dict(current)),
            )
            changed_files = _diff_config_files(staging_dir, current.data_dir)
            if changed_fields or changed_files:
                return _skip_result("tag", uuid, REASON_LOCAL_MODIFIED,
                                    {"changed_fields": changed_fields, "changed_config_files": changed_files})
        else:
            title = (backup_data.get("title") or "").strip()
            conflict = datastore.tag_uuid_for_title(title) if title else None
            if conflict:
                return _skip_result("tag", uuid, REASON_TITLE_CONFLICT, {"conflict_uuid": conflict})

        dst_dir = os.path.join(datastore.datastore_path, uuid)
        published = _publish_entity_dir(staging_dir, dst_dir)

        backup_data["uuid"] = uuid
        backup_data["processor"] = "restock_diff"
        tag_obj = Tag.model(
            datastore_path=datastore.datastore_path,
            __datastore=datastore.data,
            default=backup_data,
        )
        with datastore.lock:
            current_tags[uuid] = tag_obj
        tag_obj.commit()
        return {"entity_type": "tag", "uuid": uuid, "action_taken": published, "reason": "", "details": {}}
    except Exception as e:
        logger.exception(f"Selective restore: failed to apply tag {uuid}")
        return {"entity_type": "tag", "uuid": uuid, "action_taken": "failed", "reason": "error",
                "details": {"error": str(e)}}


def _apply_one_watch(datastore, token, uuid, local_url_index, journal):
    entities_dir = _entities_dir(datastore, token)
    staging_dir = os.path.join(entities_dir, uuid)
    current_watches = datastore.data["watching"]

    try:
        backup_data = _load_entity_json(os.path.join(staging_dir, "watch.json"))
        current = current_watches.get(uuid)
        if current is not None:
            changed_fields = _diff_signatures(
                _rehydrated_signature(datastore, "watch", uuid, backup_data),
                _rehydrated_signature(datastore, "watch", uuid, dict(current)),
            )
            changed_files = _diff_config_files(staging_dir, current.data_dir)
            if changed_fields or changed_files:
                return _skip_result("watch", uuid, REASON_LOCAL_MODIFIED,
                                    {"changed_fields": changed_fields, "changed_config_files": changed_files})
        else:
            url = (backup_data.get("url") or "").strip().lower()
            conflict = local_url_index.get(url) if url else None
            if conflict:
                return _skip_result("watch", uuid, REASON_URL_CONFLICT, {"conflict_uuid": conflict})

        dst_dir = os.path.join(datastore.datastore_path, uuid)
        published = _publish_entity_dir(staging_dir, dst_dir)

        # Drop group references that do not exist locally after the tag phase.
        current_tags = datastore.data["settings"]["application"].get("tags", {})
        requested_tags = list(backup_data.get("tags") or [])
        kept_tags = [t for t in requested_tags if t in current_tags]
        dropped_tags = [t for t in requested_tags if t not in current_tags]
        if dropped_tags:
            logger.warning(f"Selective restore: watch {uuid} drops missing group refs {dropped_tags}")
            journal["dropped_tag_refs"].append({"watch_uuid": uuid, "tag_uuids": dropped_tags})

        backup_data["uuid"] = uuid
        backup_data["tags"] = kept_tags
        watch_obj = datastore.rehydrate_entity(uuid, backup_data)
        watch_obj.ensure_valid_time_between_check()
        with datastore.lock:
            current_watches[uuid] = watch_obj
        watch_obj.commit()
        return {"entity_type": "watch", "uuid": uuid, "action_taken": published, "reason": "", "details": {}}
    except Exception as e:
        logger.exception(f"Selective restore: failed to apply watch {uuid}")
        return {"entity_type": "watch", "uuid": uuid, "action_taken": "failed", "reason": "error",
                "details": {"error": str(e)}}


def _skip_result(entity_type, uuid, reason, details):
    return {"entity_type": entity_type, "uuid": uuid, "action_taken": ACTION_SKIP,
            "reason": reason, "details": details}


def _effective_tag_selection(plan, selected_watch_uuids, selected_tag_uuids, datastore, token):
    """Explicitly selected tags plus tags required by selected watches, restricted to selectable entries."""
    selectable_tag_uuids = {e["uuid"] for e in plan["tags"] if e["selectable"]}
    effective = {u for u in selected_tag_uuids if u in selectable_tag_uuids}

    entities_dir = _entities_dir(datastore, token)
    for watch_entry in plan["watches"]:
        if watch_entry["uuid"] not in selected_watch_uuids:
            continue
        try:
            watch_data = _load_entity_json(os.path.join(entities_dir, watch_entry["uuid"], "watch.json"))
        except (json.JSONDecodeError, OSError):
            continue
        for tag_uuid in watch_data.get("tags") or []:
            if tag_uuid in selectable_tag_uuids:
                effective.add(tag_uuid)
    return effective


def apply_plan(datastore, token, selected_watch_uuids, selected_tag_uuids):
    """Apply the selected entities. Tags first, then watches. Idempotent and resumable."""
    plan = load_plan(datastore, token)
    selectable_watch_uuids = {e["uuid"] for e in plan["watches"] if e["selectable"]}
    selected_watch_uuids = {u for u in selected_watch_uuids if u in selectable_watch_uuids}
    selected_tag_uuids = _effective_tag_selection(
        plan, selected_watch_uuids, selected_tag_uuids, datastore, token
    )

    journal = {
        "status": "running",
        "started_at": int(time.time()),
        "finished_at": None,
        "backup": plan.get("backup", {}),
        "selected": {"watches": sorted(selected_watch_uuids), "tags": sorted(selected_tag_uuids)},
        "completed": {"tags": [], "watches": []},
        "results": [],
        "dropped_tag_refs": [],
        "error": None,
    }
    _write_json_atomic(_journal_path(datastore, token), journal)

    plan_entries = {("tag", e["uuid"]): e for e in plan["tags"]}
    plan_entries.update(("watch", e["uuid"]): e for e in plan["watches"])
    local_title_index = {t.get("title", "").strip().lower(): u
                         for u, t in datastore.data["settings"]["application"].get("tags", {}).items()}

    try:
        for tag_uuid in sorted(selected_tag_uuids):
            if tag_uuid in journal["completed"]["tags"]:
                result = {"entity_type": "tag", "uuid": tag_uuid, "action_taken": "already_synced",
                          "reason": "", "details": {}}
            else:
                result = _apply_one_tag(
                    datastore, token, tag_uuid, plan_entries[("tag", tag_uuid)], local_title_index, journal
                )
                if result["action_taken"] in ("added", "updated"):
                    journal["completed"]["tags"].append(tag_uuid)
            journal["results"].append(result)
            _write_json_atomic(_journal_path(datastore, token), journal)

        local_url_index = _local_url_index(datastore)
        for watch_uuid in sorted(selected_watch_uuids):
            if watch_uuid in journal["completed"]["watches"]:
                result = {"entity_type": "watch", "uuid": watch_uuid, "action_taken": "already_synced",
                          "reason": "", "details": {}}
            else:
                result = _apply_one_watch(datastore, token, watch_uuid, local_url_index, journal)
                if result["action_taken"] in ("added", "updated"):
                    journal["completed"]["watches"].append(watch_uuid)
                    # A newly added watch may itself occupy a URL needed by a later conflict check.
                    local_url_index = _local_url_index(datastore)
            journal["results"].append(result)
            _write_json_atomic(_journal_path(datastore, token), journal)

        datastore.commit()
        journal["status"] = "done"
    except Exception as e:
        logger.exception("Selective restore aborted")
        journal["status"] = "failed"
        journal["error"] = str(e)
    finally:
        journal["finished_at"] = int(time.time())
        _write_json_atomic(_journal_path(datastore, token), journal)

    return journal


def load_journal(datastore, token):
    path = _journal_path(datastore, token)
    if not os.path.isfile(path):
        return None
    return _read_json(path)


# ---------------------------------------------------------------------------
# Blueprint
# ---------------------------------------------------------------------------

_selective_threads = []
_selective_lock = threading.Lock()


def selective_restore_running():
    return any(t.is_alive() for t in _selective_threads)


def any_restore_running():
    return selective_restore_running() or bulk_restore_running()


def _journal_summary(journal):
    summary = {"added": 0, "updated": 0, "skipped": 0, "already_synced": 0, "failed": 0}
    for result in journal.get("results", []):
        summary[result["action_taken"]] = summary.get(result["action_taken"], 0) + 1
    return summary


def construct_selective_restore_blueprint(datastore):
    bp = Blueprint("selective_restore", __name__, template_folder="templates")

    @bp.route("/restore/selective", methods=["GET"])
    @login_optionally_required
    def selective_upload():
        return render_template(
            "backup_restore_selective.html",
            restore_running=any_restore_running(),
            max_upload_mb=_MAX_UPLOAD_BYTES // (1024 * 1024),
        )

    @bp.route("/restore/selective/analyze", methods=["POST"])
    @login_optionally_required
    def selective_analyze():
        if any_restore_running():
            flash(gettext("A restore is already running, check back in a few minutes"), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))

        upload = next((f for f in request.files.getlist("zip_file") if f and f.filename), None)
        if upload is None:
            flash(gettext("No file uploaded"), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))
        if not upload.filename.lower().endswith(".zip"):
            flash(gettext("File must be a .zip backup file"), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))

        content_length = request.content_length
        if content_length and content_length > _MAX_UPLOAD_BYTES:
            flash(gettext("Backup file is too large (max %(mb)s MB)",
                          mb=_MAX_UPLOAD_BYTES // (1024 * 1024)), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))

        raw = upload.read(_MAX_UPLOAD_BYTES + 1)
        if len(raw) > _MAX_UPLOAD_BYTES:
            flash(gettext("Backup file is too large (max %(mb)s MB)",
                          mb=_MAX_UPLOAD_BYTES // (1024 * 1024)), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))

        import zipfile
        try:
            with zipfile.ZipFile(_bytes_io(raw)):
                pass
            token = stage_backup(raw, datastore, upload.filename)
        except zipfile.BadZipFile:
            flash(gettext("Invalid or corrupted zip file"), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))
        except ValueError as e:
            # Zip Slip / decompressed-size guards
            flash(str(e), "error")
            return redirect(url_for("backups.selective_restore.selective_upload"))

        return redirect(url_for("backups.selective_restore.selective_preview", token=token))

    @bp.route("/restore/selective/preview/<token>", methods=["GET"])
    @login_optionally_required
    def selective_preview(token):
        validate_token(token)
        plan = load_plan(datastore, token)
        return render_template("backup_restore_preview.html", plan=plan, token=token,
                               restore_running=any_restore_running())

    @bp.route("/restore/selective/apply", methods=["POST"])
    @login_optionally_required
    def selective_apply():
        token = validate_token(request.form.get("token", ""))
        load_plan(datastore, token)  # 404 if the staging area is gone

        if any_restore_running():
            flash(gettext("A restore is already running, check back in a few minutes"), "error")
            return redirect(url_for("backups.selective_restore.selective_preview", token=token))

        selected_watches = request.form.getlist("watch_uuid")
        selected_tags = request.form.getlist("tag_uuid")
        if not selected_watches and not selected_tags:
            flash(gettext("Nothing selected, nothing to restore."), "error")
            return redirect(url_for("backups.selective_restore.selective_preview", token=token))

        thread = threading.Thread(
            target=apply_plan,
            kwargs={
                "datastore": datastore,
                "token": token,
                "selected_watch_uuids": selected_watches,
                "selected_tag_uuids": selected_tags,
            },
            daemon=True,
            name="SelectiveBackupRestore",
        )
        with _selective_lock:
            _selective_threads[:] = [t for t in _selective_threads if t.is_alive()]
            _selective_threads.append(thread)
        thread.start()
        flash(gettext("Restore started in background, check back in a few minutes."))
        return redirect(url_for("backups.selective_restore.selective_status", token=token))

    @bp.route("/restore/selective/status/<token>", methods=["GET"])
    @login_optionally_required
    def selective_status(token):
        validate_token(token)
        plan = load_plan(datastore, token)
        journal = load_journal(datastore, token)
        summary = _journal_summary(journal) if journal else None
        return render_template("backup_restore_status.html", plan=plan, journal=journal,
                               summary=summary, token=token)

    return bp
