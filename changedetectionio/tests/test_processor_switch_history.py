#!/usr/bin/env python3

"""Integration tests for carrying a watch's history across processing-mode switches.

Covers every switch entry point (edit page GET switch link, edit page POST save, REST API,
the ld+json price follower) and the startup update_35 heal for timelines that were split by
switches in older versions. The invariants under test:

- snapshots and their original timestamps/order move onto the new processor's index
- the unread state (last_viewed) survives the switch
- a failed migration leaves the old timeline fully readable and a retry neither duplicates
  nor loses anything
- snapshots produced after the switch are appended to that same single timeline
"""

import json
import os

from flask import url_for

from .util import delete_all_watches, wait_for_all_checks


def _set_text_content(datastore_path, marker):
    with open(os.path.join(datastore_path, "endpoint-content.txt"), "w") as f:
        f.write(f"<html><body>text version {marker}</body></html>")


def _set_product_content(datastore_path, price, nonce=''):
    # JSON-LD product offer so the restock processor extracts price/stock over the plain
    # html_requests fetcher (no browser needed). The nonce keeps the raw document checksum
    # changing so the check is never short-circuited as "same page as last time".
    html = f"""<html>
       <head>
       <script type="application/ld+json">
       {{"@context": "https://schema.org/", "@type": "Product", "name": "Test Product",
        "offers": {{"@type": "Offer", "priceCurrency": "USD", "price": "{price}",
                   "availability": "https://schema.org/InStock"}}}}
       </script>
       </head>
       <body><div>Available!</div><!-- {nonce} --></body>
     </html>"""
    with open(os.path.join(datastore_path, "endpoint-content.txt"), "w") as f:
        f.write(html)


def _check_now(client):
    client.post(url_for("ui.form_watch_checknow"), follow_redirects=True)
    wait_for_all_checks(client)


def _raw_index(watch, index_filename):
    with open(os.path.join(watch.data_dir, index_filename), encoding='utf-8') as f:
        return [line.strip() for line in f.readlines() if line.strip()]


def _index_path(watch, index_filename):
    return os.path.join(watch.data_dir, index_filename)


def test_get_switch_carries_history_unread_state_and_keeps_one_timeline(client, live_server,
                                                                        measure_memory_usage,
                                                                        datastore_path):
    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)

    # Build a text-mode history of three snapshots first
    uuid = datastore.add_watch(url=test_url)
    for marker in ('one', 'two', 'three'):
        _set_text_content(datastore_path, marker)
        _check_now(client)

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') in (None, 'text_json_diff')
    assert watch.history_n == 3
    original_keys = list(watch.history.keys())
    original_lines = _raw_index(watch, 'history.txt')
    original_snapshots = {k: watch.get_history_snapshot(timestamp=k) for k in original_keys}
    # Nothing viewed yet -> the watch currently has unread changes that must survive the switch
    assert watch['last_viewed'] == 0
    assert watch.has_unviewed is True

    res = client.get(
        url_for('ui.ui_edit.edit_page', uuid=uuid) + '?switch_processor=restock_diff',
        follow_redirects=True
    )
    assert b"Switched to mode" in res.data

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') == 'restock_diff'

    # One index, holding the exact same entries in the exact same order
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    assert os.path.isfile(_index_path(watch, 'history-restock_diff.txt'))
    assert _raw_index(watch, 'history-restock_diff.txt') == original_lines

    # Snapshot files and their original contents are untouched; unread state carried over
    assert watch.history_n == 3
    assert list(watch.history.keys()) == original_keys
    for key, content in original_snapshots.items():
        assert watch.get_history_snapshot(timestamp=key) == content
    assert watch['last_viewed'] == 0
    assert watch.has_unviewed is True
    assert watch.newest_history_key == original_keys[-1]

    # Checks after the switch append to the SAME (now restock) timeline. The first restock
    # check only establishes the price/stock state (no change vs nothing yet), the second one
    # actually changes the price and therefore records a new snapshot.
    _set_product_content(datastore_path, price="42.50", nonce='after-switch-1')
    _check_now(client)
    _set_product_content(datastore_path, price="39.99", nonce='after-switch-2')
    _check_now(client)

    watch = datastore.data['watching'][uuid]
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    merged_lines = _raw_index(watch, 'history-restock_diff.txt')
    assert len(merged_lines) == 4
    merged_keys = [line.split(',', 1)[0] for line in merged_lines]
    assert merged_keys[:3] == original_keys
    assert int(merged_keys[3]) > int(original_keys[-1])
    assert watch.history_n == 4
    assert 'Price: 39.99' in watch.get_history_snapshot(timestamp=merged_keys[3])

    # The processor-aware history/diff page renders the continuous timeline
    res = client.get(url_for('ui.ui_diff.diff_history_page', uuid=uuid))
    assert res.status_code == 200

    delete_all_watches(client)


def test_post_edit_switch_carries_history(client, live_server, measure_memory_usage, datastore_path):
    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)

    uuid = datastore.add_watch(url=test_url)
    for marker in ('one', 'two'):
        _set_text_content(datastore_path, marker)
        _check_now(client)

    watch = datastore.data['watching'][uuid]
    original_lines = _raw_index(watch, 'history.txt')

    res = client.post(
        url_for("ui.ui_edit.edit_page", uuid=uuid),
        data={"include_filters": "", "url": test_url, "tags": "", "headers": "",
              'fetch_backend': "html_requests",
              "time_between_check_use_default": "y",
              "processor": "restock_diff"},
        follow_redirects=True
    )
    assert b"Updated watch." in res.data

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') == 'restock_diff'
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    assert _raw_index(watch, 'history-restock_diff.txt') == original_lines
    assert watch.history_n == 2

    delete_all_watches(client)


def test_switch_failure_keeps_old_timeline_and_retry_succeeds(client, live_server,
                                                              measure_memory_usage,
                                                              datastore_path,
                                                              monkeypatch):
    from changedetectionio.model import Watch as WatchModule

    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)

    uuid = datastore.add_watch(url=test_url)
    for marker in ('one', 'two'):
        _set_text_content(datastore_path, marker)
        _check_now(client)

    watch = datastore.data['watching'][uuid]
    original_lines = _raw_index(watch, 'history.txt')

    def exploding_write(self, *args, **kwargs):
        raise OSError('simulated storage failure')

    monkeypatch.setattr(WatchModule.model, '_write_atomic', exploding_write)
    res = client.get(
        url_for('ui.ui_edit.edit_page', uuid=uuid) + '?switch_processor=restock_diff',
        follow_redirects=True
    )
    assert b"history migration failed" in res.data

    # The processor was not changed and the old timeline is fully readable; no half index
    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') in (None, 'text_json_diff')
    assert os.path.isfile(_index_path(watch, 'history.txt'))
    assert not os.path.exists(_index_path(watch, 'history-restock_diff.txt'))
    assert _raw_index(watch, 'history.txt') == original_lines
    assert watch.history_n == 2

    # Fault clears: the same switch now succeeds, losing and duplicating nothing
    monkeypatch.undo()
    res = client.get(
        url_for('ui.ui_edit.edit_page', uuid=uuid) + '?switch_processor=restock_diff',
        follow_redirects=True
    )
    assert b"Switched to mode" in res.data

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') == 'restock_diff'
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    assert _raw_index(watch, 'history-restock_diff.txt') == original_lines
    assert watch.history_n == 2

    delete_all_watches(client)


def test_api_processor_switch_carries_history(client, live_server, measure_memory_usage,
                                              datastore_path):
    datastore = client.application.config['DATASTORE']
    api_key = datastore.data['settings']['application'].get('api_access_token')
    test_url = url_for('test_endpoint', _external=True)

    uuid = datastore.add_watch(url=test_url)
    for marker in ('one', 'two'):
        _set_text_content(datastore_path, marker)
        _check_now(client)

    watch = datastore.data['watching'][uuid]
    original_keys = list(watch.history.keys())

    res = client.put(
        url_for("watch", uuid=uuid),
        headers={'x-api-key': api_key, 'content-type': 'application/json'},
        data=json.dumps({"processor": "restock_diff"})
    )
    assert res.status_code == 200, f"Processor switch via API should succeed, got: {res.data}"

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') == 'restock_diff'
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    assert list(watch.history.keys()) == original_keys

    # The history API reads the same merged timeline
    res = client.get(url_for("watchhistory", uuid=uuid), headers={'x-api-key': api_key})
    assert res.status_code == 200
    assert set(res.json.keys()) == set(original_keys)

    delete_all_watches(client)


def test_price_data_follower_accept_keeps_history(client, live_server, measure_memory_usage,
                                                  datastore_path):
    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)

    _set_text_content(datastore_path, 'before price tracking')
    uuid = datastore.add_watch(url=test_url)
    _check_now(client)

    watch = datastore.data['watching'][uuid]
    original_lines = _raw_index(watch, 'history.txt')

    # Accepting the embedded price data switches the watch to restock mode (it also queues a
    # recheck). The first restock check only establishes the price state, so change the price
    # once more to make it record a new snapshot.
    _set_product_content(datastore_path, price="19.99", nonce='accept-1')
    res = client.post(url_for('price_data_follower.accept', uuid=uuid), follow_redirects=True)
    assert res.status_code == 200
    wait_for_all_checks(client)
    _set_product_content(datastore_path, price="17.49", nonce='accept-2')
    _check_now(client)

    watch = datastore.data['watching'][uuid]
    assert watch.get('processor') == 'restock_diff'
    assert watch.get('track_ldjson_price_data') == 'accepted'
    assert not os.path.exists(_index_path(watch, 'history.txt'))

    # Old text snapshot plus the new restock snapshot share one timeline
    merged = _raw_index(watch, 'history-restock_diff.txt')
    assert len(merged) == len(original_lines) + 1
    assert merged[:len(original_lines)] == original_lines
    assert watch.history_n == len(original_lines) + 1
    assert 'Price: 17.49' in watch.get_history_snapshot(timestamp=merged[-1].split(',', 1)[0])

    delete_all_watches(client)


def test_update_35_heals_already_split_timelines(client, live_server, measure_memory_usage,
                                                 datastore_path):
    datastore = client.application.config['DATASTORE']
    test_url = url_for('test_endpoint', _external=True)

    uuid = datastore.add_watch(url=test_url)
    for marker in ('one', 'two'):
        _set_text_content(datastore_path, marker)
        _check_now(client)

    watch = datastore.data['watching'][uuid]
    text_lines = _raw_index(watch, 'history.txt')
    split_ts = max(int(k) for k in watch.history.keys()) + 10

    # Reproduce the old behaviour: switch mode without migrating, then keep checking, so the
    # snapshots accumulate in a second, invisible index
    watch['processor'] = 'restock_diff'
    watch.commit()
    watch.save_history_blob(contents='In Stock: True - Price: 9.99',
                            timestamp=split_ts,
                            snapshot_id='restock-only')

    assert os.path.isfile(_index_path(watch, 'history.txt'))
    assert os.path.isfile(_index_path(watch, 'history-restock_diff.txt'))
    # Re-reading the index shows only the restock-side entry while the timelines are split
    assert len(watch.history) == 1
    assert watch.history_n == 1

    # Startup migration folds the stale text index into the current one
    datastore.update_35()

    watch = datastore.data['watching'][uuid]
    assert not os.path.exists(_index_path(watch, 'history.txt'))
    merged = _raw_index(watch, 'history-restock_diff.txt')
    assert len(merged) == 3
    assert [line.split(',', 1)[0] for line in merged][:2] == [line.split(',', 1)[0] for line in text_lines]
    assert watch.history_n == 3
    assert int(watch.newest_history_key) == split_ts

    # Idempotent: a second startup run changes nothing and never duplicates an entry
    datastore.update_35()
    assert len(_raw_index(watch, 'history-restock_diff.txt')) == 3
    assert watch.history_n == 3

    delete_all_watches(client)
