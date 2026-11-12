"""
Persistent per-subscriber delivery cursors for the RSS feeds.

Every subscription link may carry a subscriber identifier (see
``get_subscriber_id``). For each (subscriber, feed, watch) triple we remember
the snapshot timestamp of the newest change that was put into a response for
that subscriber. The position is:

* independent - two subscriber ids never share a position, and nothing here
  touches the manual "viewed" state (``watch['last_viewed']``);
* persistent - it lives in ``rss-cursors.json`` inside the datastore, written
  atomically, so an instance restart continues at exactly the same place;
* tolerant of history cleanup - when the stored position falls behind the
  oldest snapshot still on disk, the delivery layer records an explicit
  resync instead of silently replaying or skipping.

The file is deliberately separate from ``changedetection.json`` and the
per-watch ``watch.json`` files: feed delivery positions are reader-local
state and must never be mixed with monitoring configuration or history.
"""

import json
import os
import re
import threading
import time

from loguru import logger

from changedetectionio.store.file_saving_datastore import save_json_atomic

CURSOR_FILENAME = 'rss-cursors.json'
CURSOR_SCHEMA_VERSION = 1

# A subscriber id ends up as a JSON key only, but constrain it anyway so a
# crafted URL cannot smuggle weird whitespace or control characters into the
# cursor file or log lines.
SUBSCRIBER_RE = re.compile(r'^[A-Za-z0-9._-]{1,128}$')

# Accepted query parameter names, first present (and non-empty) wins.
# ``subscriber`` is the documented name; the rest are convenience aliases.
SUBSCRIBER_PARAM_NAMES = ('subscriber', 'subscriber_id', 'sid', 'cid', 'reader_id')

# How long (seconds) a resync annotation is kept on the cursor state.
RESYNC_RECORD_TTL = 60 * 60 * 24 * 30


class InvalidSubscriberIdError(ValueError):
    """Raised when the subscriber id from the URL is not acceptable."""


def get_subscriber_id(request):
    """Return the validated subscriber id from a Flask request, or ``None``.

    ``None`` means the caller should serve the legacy, cursor-less feed.
    Raises :class:`InvalidSubscriberIdError` when an id is present but fails
    the character/length validation, so routes can answer with HTTP 400.
    """
    for name in SUBSCRIBER_PARAM_NAMES:
        value = request.args.get(name)
        if value is None:
            continue
        value = value.strip()
        if not value:
            continue
        if not SUBSCRIBER_RE.match(value):
            raise InvalidSubscriberIdError(
                "Invalid subscriber id: only letters, digits, '.', '_' and '-' are allowed (max 128 chars)"
            )
        return value
    return None


class FeedCursorStore:
    """File-backed cursor map with in-process locking and lazy loading."""

    def __init__(self, datastore_path, filename=CURSOR_FILENAME):
        self._path = os.path.join(datastore_path, filename)
        self._lock = threading.RLock()
        self._loaded = False
        self._data = {'version': CURSOR_SCHEMA_VERSION, 'subscribers': {}}

    # ------------------------------------------------------------------ I/O
    def _ensure_loaded(self):
        """Load the cursor file once, lazily. A corrupt file never takes the
        feed down - it is logged and treated as "no cursors yet"; the next
        successful commit replaces it atomically."""
        if self._loaded:
            return

        if os.path.isfile(self._path):
            try:
                with open(self._path, encoding='utf-8') as f:
                    data = f.read()
                loaded = json.loads(data)
                if isinstance(loaded, dict) and isinstance(loaded.get('subscribers'), dict):
                    self._data = {
                        'version': int(loaded.get('version', CURSOR_SCHEMA_VERSION)),
                        'subscribers': loaded['subscribers'],
                    }
                else:
                    logger.warning(f"RSS cursor file {self._path} has an unexpected shape, starting with empty cursors")
            except Exception as e:
                logger.error(f"Could not read RSS cursor file {self._path}: {e}; starting with empty cursors")
                self._data = {'version': CURSOR_SCHEMA_VERSION, 'subscribers': {}}
        self._loaded = True

    def _save_locked(self):
        """Atomically persist the current in-memory cursor map.

        Caller must hold ``self._lock``. Saving is best-effort in the same
        spirit as ``ChangeDetectionStore.commit()``: the in-memory position is
        still advanced, so a disk error does not make the feed duplicate
        entries for the lifetime of this process; the next commit retries.
        """
        try:
            save_json_atomic(self._path, self._data, label="rss-cursors")
        except Exception as e:
            logger.error(f"Failed to persist RSS cursor file {self._path}: {e}")

    @property
    def path(self):
        return self._path

    # --------------------------------------------------------------- reads
    def snapshot_cursors(self, subscriber, feed_key, watch_uuids):
        """Return ``{watch_uuid: last_delivered_timestamp_int}`` for one feed.

        Missing positions are simply absent from the dict (first contact).
        """
        with self._lock:
            self._ensure_loaded()
            feed = self._data['subscribers'].get(subscriber, {}).get('feeds', {}).get(feed_key, {})
            result = {}
            for uuid in watch_uuids:
                state = feed.get(uuid)
                if state and state.get('last_timestamp') is not None:
                    try:
                        result[uuid] = int(state['last_timestamp'])
                    except (TypeError, ValueError):
                        logger.warning(f"Ignoring non-integer RSS cursor {state.get('last_timestamp')!r} "
                                       f"for {subscriber}/{feed_key}/{uuid}")
            return result

    def get_state(self, subscriber, feed_key, watch_uuid):
        """Full cursor state (including any resync annotation), mostly for tests/diagnostics."""
        with self._lock:
            self._ensure_loaded()
            return self._data['subscribers'].get(subscriber, {}).get('feeds', {}).get(feed_key, {}).get(watch_uuid)

    # -------------------------------------------------------------- writes
    def commit_plan(self, subscriber, feed_key, items, alive_watch_uuids=None):
        """Advance cursors for a batch of planned delivery items, atomically.

        ``items`` is an iterable of dicts in delivery order (oldest change
        first), each shaped as produced by ``delivery.build_delivery_plan``:

        * ``{'kind': 'resync', 'watch_uuid': ..., 'oldest_timestamp': int,
           'previous_timestamp': int}``
        * ``{'kind': 'change', 'watch_uuid': ..., 'to_timestamp': int}``

        Items whose position was already advanced (e.g. by a concurrent pull
        of the same subscription link) are filtered out, and only the items
        actually accepted here are returned. The route renders exactly those,
        so retries never duplicate and nothing jumps over an unread change.

        ``alive_watch_uuids`` optionally prunes cursor entries for watches
        that no longer participate in this feed.
        """
        with self._lock:
            self._ensure_loaded()
            now = int(time.time())

            sub = self._data['subscribers'].setdefault(
                subscriber, {'created_at': now, 'feeds': {}}
            )
            sub.setdefault('created_at', now)
            feed = sub['feeds'].setdefault(feed_key, {})

            dirty = False
            if alive_watch_uuids is not None:
                stale = set(feed.keys()) - set(alive_watch_uuids)
                for uuid in stale:
                    feed.pop(uuid, None)
                    dirty = True

            accepted = []
            for item in items:
                if self._apply_item(feed, item, now):
                    accepted.append(item)
                    dirty = True

            # An empty retry changes nothing - don't churn the cursor file on
            # every reader poll; only real advancement (or pruning) hits disk.
            if dirty:
                self._save_locked()
            return accepted

    @staticmethod
    def _current_timestamp(state):
        """Read the integer cursor timestamp from a feed entry, or None."""
        if not state or state.get('last_timestamp') is None:
            return None
        try:
            return int(state['last_timestamp'])
        except (TypeError, ValueError):
            return None

    def _apply_item(self, feed, item, now):
        """Advance one watch cursor for one planned item; True if accepted."""
        uuid = item['watch_uuid']
        state = feed.get(uuid)
        current = self._current_timestamp(state)

        if item['kind'] == 'resync':
            oldest = int(item['oldest_timestamp'])
            # Only meaningful while the stored position really is behind the
            # oldest surviving snapshot; a first-contact subscriber gets no marker.
            if current is None or current >= oldest:
                return False
            feed[uuid] = {
                'last_timestamp': str(oldest),
                'updated_at': now,
                'resynced': {
                    'previous_timestamp': str(current),
                    'resume_timestamp': str(oldest),
                    'at': now,
                },
            }
            return True

        to_ts = int(item['to_timestamp'])
        if current is not None and to_ts <= current:
            return False

        new_state = {'last_timestamp': str(to_ts), 'updated_at': now}
        # Preserve a recent resync annotation for observability.
        if state and isinstance(state.get('resynced'), dict):
            resynced = state['resynced']
            try:
                if now - int(resynced.get('at', 0)) <= RESYNC_RECORD_TTL:
                    new_state['resynced'] = resynced
            except (TypeError, ValueError):
                pass
        feed[uuid] = new_state
        return True

    def reset_subscriber(self, subscriber):
        """Forget every position of one subscriber (used by tests/admin tooling)."""
        with self._lock:
            self._ensure_loaded()
            self._data['subscribers'].pop(subscriber, None)
            self._save_locked()
