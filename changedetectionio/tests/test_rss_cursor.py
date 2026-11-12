#!/usr/bin/env python3
"""
End-to-end tests for per-subscriber, cursor-based RSS delivery.

Covers the delivery guarantees:
* every subscription link (subscriber id) has its own persistent position;
* undelivered changes come back in real snapshot order, paged by
  rss_diff_length, without gaps or duplicates on retry;
* positions survive an instance restart;
* history cleanup is handled with an explicit resync marker and a clear
  continuation point;
* other subscribers, monitoring history and the manual "viewed" state never
  interfere with each other.
"""

import json
import os
import time
import xml.etree.ElementTree as ET

from flask import url_for

from ..blueprint.rss.cursor_store import FeedCursorStore
from .util import (
    delete_all_watches,
    extract_rss_token_from_UI,
    get_UUID_for_tag_name,
    wait_for_all_checks,
    wait_for_watch_history,
    write_test_file_and_sync,
)


def set_version(datastore_path, version):
    data = f"""<html>
    <head><title>version {version}</title></head>
    <body><p>Version {version} content - distinct body {version}</p></body>
    </html>
    """
    write_test_file_and_sync(os.path.join(datastore_path, "endpoint-content.txt"), data)


def add_versioned_watch(client, datastore_path, snapshots, delay=1.05):
    """Add a watch and drive it through `snapshots` history snapshots."""
    set_version(datastore_path, 1)
    test_url = url_for('test_endpoint', _external=True)
    uuid = client.application.config.get('DATASTORE').add_watch(url=test_url)
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)
    for version in range(2, snapshots + 1):
        set_version(datastore_path, version)
        client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
        wait_for_all_checks(client)
        # Epoch-second history keys must stay distinct for ordering assertions.
        time.sleep(delay)
    assert wait_for_watch_history(client, min_history_count=snapshots, timeout=20)
    return uuid


def parse_items(res):
    """Return [(guid, pubDate_ts)] from an RSS response, in document order."""
    root = ET.fromstring(res.data)
    items = []
    for item in root.findall('.//item'):
        guid = item.findtext('guid')
        items.append(guid)
    return items


def guid_timestamp(guid):
    return int(guid.rsplit('/', 1)[1])


def pull_single_watch(client, uuid, rss_token, subscriber=None, extra=None):
    params = {'token': rss_token}
    if subscriber is not None:
        params['subscriber'] = subscriber
    if extra:
        params.update(extra)
    res = client.get(
        url_for('rss.rss_single_watch', uuid=uuid, _external=True, **params),
        follow_redirects=False
    )
    guids = parse_items(res) if res.status_code == 200 else []
    return res, guids


def drain(client, uuid, rss_token, subscriber):
    """Pull until empty, returning every change GUID delivered, in order."""
    seen = []
    while True:
        _, guids = pull_single_watch(client, uuid, rss_token, subscriber)
        if not guids:
            return seen
        seen.extend(guids)


def test_two_subscribers_are_independent_and_retries_do_not_duplicate(client, live_server, measure_memory_usage, datastore_path):
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=4)  # 3 changes

    # First contact: each subscriber gets the whole backlog, oldest first.
    res, guids_a = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert res.status_code == 200
    assert len(guids_a) == 3
    assert [guid_timestamp(g) for g in guids_a] == sorted(guid_timestamp(g) for g in guids_a)

    res, guids_b = pull_single_watch(client, uuid, rss_token, 'reader-b')
    assert guids_b == guids_a, "a second subscriber must have its own independent position"

    # Same pull retried before anything changed: nothing is re-delivered.
    _, retry_a = pull_single_watch(client, uuid, rss_token, 'reader-a')
    _, retry_b = pull_single_watch(client, uuid, rss_token, 'reader-b')
    assert retry_a == [] and retry_b == []

    # A new change arrives: each subscriber receives exactly that one, once.
    set_version(datastore_path, 5)
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)

    _, new_a = pull_single_watch(client, uuid, rss_token, 'reader-a')
    _, new_b = pull_single_watch(client, uuid, rss_token, 'reader-b')
    assert len(new_a) == 1 and len(new_b) == 1
    assert new_a == new_b
    assert guid_timestamp(new_a[0]) > guid_timestamp(guids_a[-1])

    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []
    assert pull_single_watch(client, uuid, rss_token, 'reader-b')[1] == []

    delete_all_watches(client)


def test_paging_follows_snapshot_order_without_skipping_or_repeating(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    datastore.data['settings']['application']['rss_diff_length'] = 2
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=6)  # 5 changes

    expected = [f"{uuid}/{ts}" for ts in sorted(int(k) for k in datastore.data['watching'][uuid].history.keys())[1:]]

    pages = []
    delivered = []
    for _ in range(10):
        _, guids = pull_single_watch(client, uuid, rss_token, 'reader-a')
        if not guids:
            break
        pages.append(guids)
        delivered.extend(guids)

    # Page size is honoured (last page may be short)...
    assert [len(p) for p in pages] == [2, 2, 1]
    # ...every change arrives exactly once and in true snapshot order.
    assert delivered == expected
    # Retrying the empty pull stays empty.
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    delete_all_watches(client)


def test_cursor_position_persists_across_restart(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    datastore.data['settings']['application']['rss_diff_length'] = 2
    # Commit so the page size survives reload_state() like a real saved setting.
    datastore.commit()
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=5)  # 4 changes

    _, first_page = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert len(first_page) == 2

    cursor_file = os.path.join(datastore_path, 'rss-cursors.json')
    assert os.path.isfile(cursor_file)
    on_disk = json.load(open(cursor_file, encoding='utf-8'))
    assert on_disk['subscribers']['reader-a']['feeds'][f'watch:{uuid}'][uuid]['last_timestamp']

    # Simulate an instance restart: datastore reloaded from disk and a brand
    # new cursor store object (what the next process would construct).
    datastore.reload_state(
        datastore_path=datastore_path,
        include_default_watches=False,
        version_tag=datastore.data.get('version_tag', '0.0.0')
    )
    assert datastore.data['settings']['application']['rss_diff_length'] == 2
    restarted_store = FeedCursorStore(datastore_path)
    assert restarted_store.snapshot_cursors('reader-a', f'watch:{uuid}', [uuid]) != {}

    # The live link continues exactly where it stopped - no replay.
    _, second_page = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert len(second_page) == 2
    assert set(second_page).isdisjoint(set(first_page))
    assert guid_timestamp(second_page[0]) > guid_timestamp(first_page[-1])
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    # A fresh subscriber still starts from the oldest available change, paged.
    _, fresh = pull_single_watch(client, uuid, rss_token, 'reader-b')
    assert len(fresh) == 2
    assert guid_timestamp(fresh[0]) < guid_timestamp(second_page[0])

    delete_all_watches(client)


def test_history_trim_emits_resync_marker_then_continues(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    datastore.data['settings']['application']['rss_diff_length'] = 1
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=4)  # 3 changes
    watch = datastore.data['watching'][uuid]
    original_dates = sorted(int(k) for k in watch.history.keys())

    # Deliver only the oldest change; the cursor now sits on original_dates[1].
    _, first = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert guid_timestamp(first[0]) == original_dates[1]

    # History cleanup keeps just the two newest snapshots, both newer than cursor.
    watch.history_trim(newest_n_items=2)
    surviving = sorted(int(k) for k in watch.history.keys())
    assert surviving == original_dates[-2:]

    # Page size is 1, so the resync marker occupies the first pull by itself...
    res, marker_page = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert res.status_code == 200
    assert marker_page == [f"{uuid}/resync/{surviving[0]}"]
    root = ET.fromstring(res.data)
    assert 'history cleanup' in root.findall('.//item')[0].findtext('title')

    # ...and the still-pending real change comes on the next pull, in order.
    _, change_page = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert change_page == [f"{uuid}/{surviving[1]}"]

    # Marker is one-shot and the position is now caught up.
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []
    state = json.load(open(os.path.join(datastore_path, 'rss-cursors.json'), encoding='utf-8'))
    cursor_state = state['subscribers']['reader-a']['feeds'][f'watch:{uuid}'][uuid]
    assert cursor_state['last_timestamp'] == str(surviving[1])
    assert cursor_state['resynced']['resume_timestamp'] == str(surviving[0])

    # A different subscriber is independent: first contact after the trim has
    # no gap to reconcile (it never had a position), so it gets only real changes.
    _, guids_b = pull_single_watch(client, uuid, rss_token, 'reader-b')
    assert guids_b == [f"{uuid}/{surviving[1]}"]
    assert all('/resync/' not in g for g in guids_b)

    delete_all_watches(client)


def test_cleared_history_resyncs_when_new_changes_grow(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=3)  # 2 changes

    delivered = drain(client, uuid, rss_token, 'reader-a')
    assert len(delivered) == 2

    # Wipe all snapshots: nothing to deliver, but the cursor must be retained.
    datastore.clear_watch_history(uuid)
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    # First snapshot after the wipe alone is not a change...
    set_version(datastore_path, 90)
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    # ...the second one forms a new diff behind a resync marker.
    set_version(datastore_path, 91)
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)
    res, guids = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert len(guids) == 2
    assert '/resync/' in guids[0]
    assert '/resync/' not in guids[1]
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    delete_all_watches(client)


def test_cursor_delivery_ignores_manual_viewed_state(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    datastore.data['settings']['application']['rss_diff_length'] = 2
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=4)  # 3 changes
    watch = datastore.data['watching'][uuid]

    # RSS pulls never touch the manual viewed state.
    _, first = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert len(first) == 2
    assert int(watch['last_viewed']) == 0
    assert watch.viewed is False

    # Marking everything read in the UI does not steal the reader's backlog.
    client.post(url_for('ui.mark_all_viewed'), follow_redirects=True)
    wait_for_all_checks(client)
    assert watch.viewed is True

    # The legacy, subscriber-less feed is driven by the viewed flag, so it is
    # now empty - proving it is a separate mechanism from the cursor positions.
    legacy = client.get(url_for('rss.feed', token=rss_token), follow_redirects=False)
    assert legacy.status_code == 200
    assert parse_items(legacy) == []

    _, remaining = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert len(remaining) == 1
    assert set(remaining).isdisjoint(set(first))
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    # Marking unviewed again must not replay already-delivered cursor entries,
    # even though the legacy feed exposes the change again.
    client.post(url_for("ui.form_watch_list_checkbox_operations"),
                data={"op": "mark-unviewed", "uuids": uuid}, follow_redirects=True)
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []
    legacy_reopened = client.get(url_for('rss.feed', token=rss_token), follow_redirects=False)
    assert len(parse_items(legacy_reopened)) == 1

    delete_all_watches(client)


def test_main_and_tag_feeds_keep_independent_cursor_positions(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config.get('DATASTORE')
    # One big page so the assertions don't depend on scheduler-created snapshots.
    datastore.data['settings']['application']['rss_diff_length'] = 50
    rss_token = extract_rss_token_from_UI(client)

    # Two tagged watches, each driven through several versions. The unique
    # query string is how we map them back to their UUIDs afterwards.
    for watch_n in (1, 2):
        set_version(datastore_path, f"1-{watch_n}")
        res = client.post(
            url_for("ui.ui_views.form_quick_watch_add"),
            data={"url": url_for('test_endpoint', _external=True) + f"?w={watch_n}", "tags": 'cursor-tag'},
            follow_redirects=True
        )
        assert b"Watch added" in res.data
        wait_for_all_checks(client)
        for version in (2, 3):
            set_version(datastore_path, f"{version}-{watch_n}")
            client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
            wait_for_all_checks(client)
            time.sleep(1.05)

    watch_uuids = [
        uuid for uuid, w in datastore.data['watching'].items()
        if '?w=' in (w.get('url') or '')
    ]
    assert len(watch_uuids) == 2

    # Derive the expected change GUIDs from the actual on-disk history, so an
    # extra scheduler-triggered snapshot can't make the assertion brittle.
    expected = set()
    for one_uuid in watch_uuids:
        keys = sorted(int(k) for k in datastore.data['watching'][one_uuid].history.keys())
        expected.update(f"{one_uuid}/{ts}" for ts in keys[1:])
    assert expected, "the fixtures should have produced at least one change"

    tag_uuid = get_UUID_for_tag_name(client, 'cursor-tag')
    assert tag_uuid

    def pull_tag(subscriber):
        res = client.get(
            url_for('rss.rss_tag_feed', tag_uuid=tag_uuid, token=rss_token, subscriber=subscriber),
            follow_redirects=False
        )
        return res, parse_items(res)

    def pull_main(subscriber):
        res = client.get(
            url_for('rss.feed', token=rss_token, subscriber=subscriber),
            follow_redirects=True
        )
        return res, parse_items(res)

    # Tag link: every change of the tagged watches, merged in snapshot order.
    res, tag_guids = pull_tag('reader-a')
    assert res.status_code == 200
    tag_changes = [g for g in tag_guids if '/resync/' not in g]
    assert set(tag_changes) == expected
    assert [guid_timestamp(g) for g in tag_changes] == sorted(guid_timestamp(g) for g in tag_changes)
    assert pull_tag('reader-a')[1] == []

    # The same subscriber on the main feed keeps a separate cursor and gets
    # its own full backlog; the tag pull advanced nothing on it.
    res, main_guids = pull_main('reader-a')
    main_changes = [g for g in main_guids if '/resync/' not in g]
    assert set(main_changes) == expected
    assert pull_main('reader-a')[1] == []

    # Manual viewed state does not move either cursor.
    client.post(url_for('ui.mark_all_viewed'), follow_redirects=True)
    wait_for_all_checks(client)
    assert pull_tag('reader-a')[1] == []
    assert pull_main('reader-a')[1] == []

    # A second subscriber on the tag link starts fresh.
    _, other = pull_tag('reader-b')
    other_changes = [g for g in other if '/resync/' not in g]
    assert set(other_changes) == expected

    delete_all_watches(client)


def test_subscriber_validation_token_and_aliases(client, live_server, measure_memory_usage, datastore_path):
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=2)

    # Bad subscriber ids are rejected on every feed type.
    res, _ = pull_single_watch(client, uuid, rss_token, 'bad id with spaces')
    assert res.status_code == 400
    res = client.get(url_for('rss.feed', token=rss_token, subscriber='bad/id'), follow_redirects=True)
    assert res.status_code == 400

    # A bad token still loses, even with a well-formed subscriber id.
    res, _ = pull_single_watch(client, uuid, 'wrong-token', 'reader-a')
    assert res.status_code == 403

    # Alias parameter names behave exactly like subscriber=.
    res_alias, guids_alias = pull_single_watch(client, uuid, rss_token, extra={'cid': 'alias-reader'})
    res_main, guids_main = pull_single_watch(client, uuid, rss_token, 'alias-reader')
    assert res_alias.status_code == 200 and res_main.status_code == 200
    assert guids_alias and guids_main == [], "the cid= alias must resolve to the same stored position"

    delete_all_watches(client)


def test_resubscribing_same_link_does_not_replay_old_entries(client, live_server, measure_memory_usage, datastore_path):
    rss_token = extract_rss_token_from_UI(client)
    uuid = add_versioned_watch(client, datastore_path, snapshots=4)  # 3 changes

    first = drain(client, uuid, rss_token, 'reader-a')
    assert len(first) == 3

    # "Re-subscribe" with the exact same link: position is remembered, no replay.
    assert pull_single_watch(client, uuid, rss_token, 'reader-a')[1] == []

    # A brand-new subscription link starts from the oldest available change.
    res, fresh = pull_single_watch(client, uuid, rss_token, 'brand-new-reader')
    assert len(fresh) == 3
    assert guid_timestamp(fresh[0]) == guid_timestamp(first[0])

    delete_all_watches(client)


def test_single_watch_cursor_mode_returns_empty_feed_before_first_change(client, live_server, measure_memory_usage, datastore_path):
    rss_token = extract_rss_token_from_UI(client)
    set_version(datastore_path, 1)
    uuid = client.application.config.get('DATASTORE').add_watch(
        url=url_for('test_endpoint', _external=True)
    )
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)

    # One snapshot, zero changes: cursor mode answers 200 with an empty feed,
    # while the legacy route keeps its 400 contract.
    res, guids = pull_single_watch(client, uuid, rss_token, 'reader-a')
    assert res.status_code == 200 and guids == []

    legacy, _ = pull_single_watch(client, uuid, rss_token, None)
    assert legacy.status_code == 400

    delete_all_watches(client)
