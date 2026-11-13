#!/usr/bin/env python3

"""Unit tests for Watch.migrate_history_to_processor(): carrying a watch's snapshot timeline
onto another processor's history index when the processing mode changes.

Covers content/order preservation, idempotent reruns, crash-safety (old index stays readable
on failure), same-timestamp collisions and unread-state preservation - all without a live server.
"""

import os
import tempfile

import pytest

from changedetectionio.model import Watch as WatchModule


def _make_watch(datastore_path):
    mock_datastore = {
        'settings': {'application': {}},
        'watching': {}
    }
    watch = WatchModule.model(datastore_path=datastore_path, __datastore=mock_datastore, default={})
    watch.ensure_data_dir_exists()
    return watch


def _read_index_lines(watch, index_filename):
    path = os.path.join(watch.data_dir, index_filename)
    with open(path, encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]


def test_migrate_merges_entries_keeps_order_and_snapshot_files():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        for ts, snapshot_id, content in (
            (100, 'snap-a', 'first version'),
            (200, 'snap-b', 'second version'),
            (300, 'snap-c', 'third version'),
        ):
            watch.save_history_blob(contents=content, timestamp=ts, snapshot_id=snapshot_id)

        assert os.path.isfile(os.path.join(watch.data_dir, 'history.txt'))

        added = watch.migrate_history_to_processor('restock_diff')

        assert added == 3
        # The old timeline index is retired, the new one holds the same entries in the same order
        assert not os.path.exists(os.path.join(watch.data_dir, 'history.txt'))
        target = os.path.join(watch.data_dir, 'history-restock_diff.txt')
        assert os.path.isfile(target)

        lines = _read_index_lines(watch, 'history-restock_diff.txt')
        assert [line.split(',', 1)[0] for line in lines] == ['100', '200', '300']
        assert [line.split(',', 1)[1] for line in lines] == ['snap-a.txt', 'snap-b.txt', 'snap-c.txt']

        # Snapshot files are never moved or deleted and stay readable through the watch API
        watch['processor'] = 'restock_diff'
        history = watch.history
        assert list(history.keys()) == ['100', '200', '300']
        assert watch.history_n == 3
        assert watch.newest_history_key == '300'
        assert watch.get_history_snapshot(timestamp='200') == 'second version'
        snapshot_files = [f for f in os.listdir(watch.data_dir) if f.endswith('.txt') and f.startswith('snap-')]
        assert sorted(snapshot_files) == ['snap-a.txt', 'snap-b.txt', 'snap-c.txt']


def test_migrate_is_idempotent_and_reversible():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        for ts in (100, 200, 300):
            watch.save_history_blob(contents=f"content {ts}", timestamp=ts, snapshot_id=f'snap-{ts}')

        watch.migrate_history_to_processor('restock_diff')

        # Rerun after the source index was retired: nothing to do, target must not gain duplicates
        assert watch.migrate_history_to_processor('restock_diff') == 0
        assert len(_read_index_lines(watch, 'history-restock_diff.txt')) == 3

        # Switching back carries the whole continuous timeline back to history.txt
        watch['processor'] = 'restock_diff'
        added_back = watch.migrate_history_to_processor('text_json_diff')
        assert added_back == 3
        assert not os.path.exists(os.path.join(watch.data_dir, 'history-restock_diff.txt'))
        assert len(_read_index_lines(watch, 'history.txt')) == 3
        # And again: stable, no duplicates
        assert watch.migrate_history_to_processor('text_json_diff') == 0
        assert len(_read_index_lines(watch, 'history.txt')) == 3


def test_new_snapshots_after_switch_share_the_same_timeline():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        watch.save_history_blob(contents='old text one', timestamp=100, snapshot_id='a')
        watch.save_history_blob(contents='old text two', timestamp=200, snapshot_id='b')

        watch.migrate_history_to_processor('restock_diff')
        watch['processor'] = 'restock_diff'

        # A check performed after the switch appends to the same (now restock) index
        watch.save_history_blob(contents='In Stock: True - Price: 12.99', timestamp=300, snapshot_id='c')

        lines = _read_index_lines(watch, 'history-restock_diff.txt')
        assert [line.split(',', 1)[0] for line in lines] == ['100', '200', '300']
        assert watch.history_n == 3
        # save_history_blob() seeds the cached key with the raw (int) timestamp it was given
        assert str(watch.newest_history_key) == '300'
        assert watch.get_history_snapshot(timestamp='300') == 'In Stock: True - Price: 12.99'
        # The old snapshots remain readable in the same timeline
        assert watch.get_history_snapshot(timestamp='100') == 'old text one'


def test_migrate_unions_existing_target_without_duplicates():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)

        # Old text-mode timeline
        watch.save_history_blob(contents='text 100', timestamp=100, snapshot_id='old-a')
        watch.save_history_blob(contents='text 200', timestamp=200, snapshot_id='old-b')

        # The watch was switched already and accumulated restock snapshots in its own index
        watch['processor'] = 'restock_diff'
        watch.save_history_blob(contents='In Stock: True - Price: 1.0', timestamp=300, snapshot_id='new-a')

        # Healing the split: merge the stale text index into the current one
        watch['processor'] = 'text_json_diff'
        added = watch.migrate_history_to_processor('restock_diff')
        assert added == 2

        watch['processor'] = 'restock_diff'
        assert list(watch.history.keys()) == ['100', '200', '300']
        assert watch.history_n == 3

        # Rerunning the heal must not duplicate the carried entries
        watch['processor'] = 'text_json_diff'
        assert watch.migrate_history_to_processor('restock_diff') == 0
        watch['processor'] = 'restock_diff'
        assert watch.history_n == 3


def test_migrate_failure_keeps_old_timeline_readable():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        for ts in (100, 200, 300):
            watch.save_history_blob(contents=f"content {ts}", timestamp=ts, snapshot_id=f'snap-{ts}')

        def boom(*args, **kwargs):
            raise OSError('simulated disk failure')

        # Simulate a crash while writing the target index
        watch._write_atomic = boom
        with pytest.raises(OSError):
            watch.migrate_history_to_processor('restock_diff')

        # The source timeline is fully intact and the target index was never created
        source_lines = _read_index_lines(watch, 'history.txt')
        assert len(source_lines) == 3
        assert not os.path.exists(os.path.join(watch.data_dir, 'history-restock_diff.txt'))
        for ts in (100, 200, 300):
            assert watch.get_history_snapshot(timestamp=str(ts)) == f'content {ts}'

        # Retry after the fault clears succeeds without duplicating or losing anything
        del watch._write_atomic
        assert watch.migrate_history_to_processor('restock_diff') == 3
        watch['processor'] = 'restock_diff'
        assert watch.history_n == 3
        assert list(watch.history.keys()) == ['100', '200', '300']


def test_migrate_same_processor_and_missing_source_are_noops():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        assert watch.migrate_history_to_processor('text_json_diff') == 0

        watch.save_history_blob(contents='only one', timestamp=100, snapshot_id='a')
        watch.migrate_history_to_processor('restock_diff')

        # Source index gone: a second switch attempt has nothing to merge
        watch['processor'] = 'restock_diff'
        assert watch.migrate_history_to_processor('restock_diff', from_processor='text_json_diff') == 0


def test_migrate_preserves_unread_state():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        for ts in (100, 200, 300):
            watch.save_history_blob(contents=f"content {ts}", timestamp=ts, snapshot_id=f'snap-{ts}')

        # User has only seen the oldest snapshot; the newer two are unread
        watch['last_viewed'] = 150
        assert watch.has_unviewed is True

        watch.migrate_history_to_processor('restock_diff')
        watch['processor'] = 'restock_diff'

        # last_viewed is a watch field and is compared against the same timestamps, so the
        # unread state survives the switch untouched
        assert watch['last_viewed'] == 150
        assert watch.has_unviewed is True
        assert watch.viewed is False
        assert watch.history_n == 3

        watch['last_viewed'] = 300
        assert watch.viewed is True
        assert watch.has_unviewed is False


def test_migrate_same_timestamp_collision_keeps_target_and_orphans_nothing_deleted():
    with tempfile.TemporaryDirectory() as datastore_path:
        watch = _make_watch(datastore_path)
        watch.save_history_blob(contents='text version at 100', timestamp=100, snapshot_id='text-a')
        watch.save_history_blob(contents='text version at 200', timestamp=200, snapshot_id='text-b')

        # Same epoch second under the new processor (worker normally avoids this, migration must
        # still behave deterministically if it happens)
        watch['processor'] = 'restock_diff'
        watch.save_history_blob(contents='In Stock: True - Price: 1.0', timestamp=100, snapshot_id='restock-a')
        watch.save_history_blob(contents='In Stock: True - Price: 2.0', timestamp=300, snapshot_id='restock-b')

        watch['processor'] = 'text_json_diff'
        added = watch.migrate_history_to_processor('restock_diff')
        # Only 200 is new; the colliding 100 keeps the target's own entry and is not double-counted
        assert added == 1

        watch['processor'] = 'restock_diff'
        assert list(watch.history.keys()) == ['100', '200', '300']
        assert watch.get_history_snapshot(timestamp='100') == 'In Stock: True - Price: 1.0'
        # The losing source snapshot stays on disk, unreferenced but not deleted
        assert os.path.isfile(os.path.join(watch.data_dir, 'text-a.txt'))


def test_history_index_filename_for_mapping():
    assert WatchModule.model.history_index_filename_for(None) == 'history.txt'
    assert WatchModule.model.history_index_filename_for('') == 'history.txt'
    assert WatchModule.model.history_index_filename_for('text_json_diff') == 'history.txt'
    assert WatchModule.model.history_index_filename_for('restock_diff') == 'history-restock_diff.txt'
