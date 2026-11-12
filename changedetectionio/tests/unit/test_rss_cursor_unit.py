#!/usr/bin/env python3
"""Unit tests for the per-subscriber RSS cursor store and delivery planner.

These run without a live server: fake watches expose just the surface the
planner touches (uuid/url + an ordered history index).
"""

import json
import os

import pytest
from flask import Flask

from changedetectionio.blueprint.rss.cursor_store import (
    FeedCursorStore,
    InvalidSubscriberIdError,
    get_subscriber_id,
)
from changedetectionio.blueprint.rss.delivery import (
    FEED_KEY_MAIN,
    build_delivery_plan,
    feed_key_for_tag,
    feed_key_for_watch,
    plan_pending_for_watch,
    resync_guid,
)


class FakeWatch(dict):
    """Minimal stand-in for model.Watch: dict fields plus a `.history` map."""

    def __init__(self, uuid, timestamps):
        super().__init__({'uuid': uuid, 'url': f'https://example.com/{uuid}'})
        self._history = {str(ts): f'/fake/{uuid}/{ts}.txt' for ts in timestamps}

    @property
    def history(self):
        return dict(self._history)


def _store(tmp_path):
    return FeedCursorStore(str(tmp_path))


def test_plan_first_contact_delivers_everything_oldest_first():
    watch = FakeWatch('w1', [100, 200, 300, 400])
    resync, pairs = plan_pending_for_watch(watch, None)
    assert resync is None
    assert pairs == [('100', '200'), ('200', '300'), ('300', '400')]


def test_plan_caught_up_returns_nothing():
    watch = FakeWatch('w1', [100, 200, 300])
    assert plan_pending_for_watch(watch, 300) == (None, [])
    # A cursor past the newest snapshot (clock skew / trimmed tail) must not replay anything.
    assert plan_pending_for_watch(watch, 999) == (None, [])


def test_plan_normal_pending_only_newer_snapshots():
    watch = FakeWatch('w1', [100, 200, 300, 400])
    resync, pairs = plan_pending_for_watch(watch, 200)
    assert resync is None
    assert pairs == [('200', '300'), ('300', '400')]


def test_plan_single_snapshot_has_no_changes():
    watch = FakeWatch('w1', [100])
    assert plan_pending_for_watch(watch, None) == (None, [])
    assert plan_pending_for_watch(watch, 50) == (None, [])


def test_plan_cursor_before_oldest_emits_resync():
    # History cleanup removed everything the cursor used to point at.
    watch = FakeWatch('w1', [300, 400, 500])
    resync, pairs = plan_pending_for_watch(watch, 100)
    assert resync == {'previous_timestamp': 100, 'oldest_timestamp': 300}
    assert pairs == [('300', '400'), ('400', '500')]


def test_plan_merged_stream_is_in_snapshot_order_and_paged(tmp_path):
    store = _store(tmp_path)
    # Interleaved snapshots across two watches.
    w1 = FakeWatch('w1', [100, 200, 400])
    w2 = FakeWatch('w2', [150, 300, 500])

    page1 = build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [w1, w2], batch_limit=3)
    guids = [f"{i['watch_uuid']}/{i['to_timestamp']}" for i in page1 if i['kind'] == 'change']
    assert guids == ['w1/200', 'w2/300', 'w1/400'], "merged stream must follow real snapshot order"

    # The second page continues exactly where page one ended - nothing skipped.
    page2 = build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [w1, w2], batch_limit=3)
    guids2 = [f"{i['watch_uuid']}/{i['to_timestamp']}" for i in page2 if i['kind'] == 'change']
    assert guids2 == ['w2/500']

    # Retrying with no new snapshots returns nothing (no duplicates).
    assert build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [w1, w2], batch_limit=3) == []


def test_planner_subscribers_and_feeds_are_independent(tmp_path):
    store = _store(tmp_path)
    w1 = FakeWatch('w1', [100, 200, 300])

    first_a = build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [w1], batch_limit=1)
    first_b = build_delivery_plan(None, store, 'reader-b', FEED_KEY_MAIN, [w1], batch_limit=1)
    assert first_a and first_b and first_a[0] == first_b[0]

    # Same watch reached via the single-watch link keeps a separate position.
    first_watch = build_delivery_plan(None, store, 'reader-a', feed_key_for_watch('w1'), [w1], batch_limit=1)
    assert first_watch[0]['to_timestamp'] == '200'

    # Tag link is independent too.
    first_tag = build_delivery_plan(None, store, 'reader-a', feed_key_for_tag('t1'), [w1], batch_limit=1)
    assert first_tag[0]['to_timestamp'] == '200'

    # Advancing one feed never moved the others.
    cursors_main = store.snapshot_cursors('reader-a', FEED_KEY_MAIN, ['w1'])
    cursors_watch = store.snapshot_cursors('reader-a', feed_key_for_watch('w1'), ['w1'])
    cursors_tag = store.snapshot_cursors('reader-a', feed_key_for_tag('t1'), ['w1'])
    assert cursors_main == {'w1': 200}
    assert cursors_watch == {'w1': 200}
    assert cursors_tag == {'w1': 200}
    assert store.snapshot_cursors('reader-b', FEED_KEY_MAIN, ['w1']) == {'w1': 200}


def test_resync_marker_is_first_persisted_and_one_shot(tmp_path):
    store = _store(tmp_path)
    feed_key = feed_key_for_watch('w1')
    # Seed a delivered position at 100, as if earlier pulls had consumed it.
    store.commit_plan('reader-a', feed_key, [
        {'kind': 'change', 'watch_uuid': 'w1', 'to_timestamp': 100}
    ])
    # History cleanup leaves only [300, 400]; the cursor (100) is now behind
    # the oldest surviving snapshot.
    watch = FakeWatch('w1', [300, 400])
    items = build_delivery_plan(None, store, 'reader-a', feed_key, [watch], batch_limit=5)
    assert items[0]['kind'] == 'resync'
    assert items[0]['oldest_timestamp'] == 300
    assert items[0]['previous_timestamp'] == 100
    # Marker sorts before the real surviving change.
    assert [i['kind'] for i in items] == ['resync', 'change']

    state = store.get_state('reader-a', feed_key_for_watch('w1'), 'w1')
    assert state['last_timestamp'] == '400'
    assert state['resynced']['previous_timestamp'] == '100'
    assert state['resynced']['resume_timestamp'] == '300'

    # Retry: marker is not re-emitted and nothing duplicates.
    assert build_delivery_plan(None, store, 'reader-a', feed_key_for_watch('w1'), [watch], batch_limit=5) == []


def test_resync_guid_is_stable():
    assert resync_guid('w1', 300) == 'w1/resync/300'


def test_cursor_file_survives_a_new_store_instance(tmp_path):
    store = _store(tmp_path)
    watch = FakeWatch('w1', [100, 200, 300])
    build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [watch], batch_limit=0)

    path = os.path.join(str(tmp_path), 'rss-cursors.json')
    assert os.path.isfile(path)
    on_disk = json.load(open(path, encoding='utf-8'))
    assert on_disk['subscribers']['reader-a']['feeds'][FEED_KEY_MAIN]['w1']['last_timestamp'] == '300'

    # A brand-new instance (what a restarted process creates) resumes from disk.
    restarted = _store(tmp_path)
    assert restarted.snapshot_cursors('reader-a', FEED_KEY_MAIN, ['w1']) == {'w1': 300}
    assert build_delivery_plan(None, restarted, 'reader-a', FEED_KEY_MAIN, [watch], batch_limit=5) == []


def test_corrupt_cursor_file_is_tolerated(tmp_path):
    path = os.path.join(str(tmp_path), 'rss-cursors.json')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('{not valid json')

    store = _store(tmp_path)
    watch = FakeWatch('w1', [100, 200])
    items = build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [watch], batch_limit=5)
    assert len(items) == 1 and items[0]['kind'] == 'change'
    # The successful commit replaced the broken file atomically.
    assert json.load(open(path, encoding='utf-8'))['subscribers']


def test_stale_watch_cursors_are_pruned_from_feed(tmp_path):
    store = _store(tmp_path)
    build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN,
                        [FakeWatch('w1', [100, 200]), FakeWatch('w2', [100, 200])], batch_limit=0)
    # w2 disappears (deleted watch); the next pull for this feed prunes its cursor.
    build_delivery_plan(None, store, 'reader-a', FEED_KEY_MAIN, [FakeWatch('w1', [100, 200, 300])], batch_limit=0)
    assert store.snapshot_cursors('reader-a', FEED_KEY_MAIN, ['w1', 'w2']) == {'w1': 300}


def test_get_subscriber_id_validation():
    app = Flask(__name__)
    with app.test_request_context('/rss?subscriber=reader-A_1.2'):
        from flask import request
        assert get_subscriber_id(request) == 'reader-A_1.2'

    with app.test_request_context('/rss?cid=alias-reader'):
        from flask import request
        assert get_subscriber_id(request) == 'alias-reader'

    with app.test_request_context('/rss'):
        from flask import request
        assert get_subscriber_id(request) is None

    with app.test_request_context('/rss?subscriber=bad%20id'):
        from flask import request
        with pytest.raises(InvalidSubscriberIdError):
            get_subscriber_id(request)

    with app.test_request_context('/rss?subscriber=' + 'x' * 129):
        from flask import request
        with pytest.raises(InvalidSubscriberIdError):
            get_subscriber_id(request)


def test_batch_limit_zero_means_unlimited(tmp_path):
    store = _store(tmp_path)
    watch = FakeWatch('w1', list(range(100, 600, 100)))  # 5 snapshots, 4 changes
    items = build_delivery_plan(None, store, 'reader-a', feed_key_for_watch('w1'), [watch], batch_limit=0)
    assert len([i for i in items if i['kind'] == 'change']) == 4
