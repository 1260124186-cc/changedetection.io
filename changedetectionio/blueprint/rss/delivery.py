"""
Cursor-based, per-subscriber delivery planning for the RSS feeds.

The legacy feeds answer from the shared ``watch.viewed`` flag or from a
"last N snapshots" window, so two readers of the same instance steal each
other's changes and a re-subscription replays old entries.

The cursor mode instead treats every consecutive pair of history snapshots
as one immutable change (GUID ``<watch-uuid>/<to-timestamp>``, matching the
legacy feeds) and, for each subscriber independently, returns the changes
whose "to" snapshot is newer than that subscriber's persisted position:

* changes are returned in real snapshot order, oldest undelivered first;
* a pull only advances the position over the entries it actually contains,
  bounded by the ``rss_diff_length`` page size, so a backlog is paged
  through without skipping anything;
* retrying the very same pull before new snapshots exist finds nothing
  pending and returns an empty feed - no duplicates;
* when history cleanup removes snapshots the cursor still points at, an
  explicit resync marker is emitted once and delivery continues from the
  oldest snapshot that remains.

The planning functions here are pure data work; the routes own rendering.
"""

import datetime

import pytz
from loguru import logger

from ._util import add_watch_categories

# Feed keys identify which subscription link a cursor belongs to.
FEED_KEY_MAIN = 'main'


def feed_key_for_watch(watch_uuid):
    return f'watch:{watch_uuid}'


def feed_key_for_tag(tag_uuid):
    return f'tag:{tag_uuid}'


def resync_guid(watch_uuid, oldest_timestamp):
    """Stable GUID for the one-off "your position was reset" marker entry."""
    return f'{watch_uuid}/resync/{int(oldest_timestamp)}'


def _sorted_history_keys(watch):
    """History index keys (epoch seconds as strings) in true snapshot order."""
    return sorted(watch.history.keys(), key=int)


def plan_pending_for_watch(watch, cursor_timestamp):
    """Plan the undelivered changes of one watch.

    Returns ``(resync, pairs)`` where:

    * ``resync`` is ``None`` or
      ``{'previous_timestamp': int, 'oldest_timestamp': int}`` describing a
      history gap in front of the stored cursor;
    * ``pairs`` is a list of ``(from_timestamp, to_timestamp)`` snapshot
      pairs (string keys, as stored in the history index) in ascending
      order, every one of them still undelivered for this subscriber.

    No page limit is applied here; the feed-level planner slices the merged
    stream once, so ordering across watches stays correct.
    """
    dates = _sorted_history_keys(watch)
    # A single snapshot means no change has ever been detected.
    if len(dates) < 2:
        return None, []

    oldest = int(dates[0])
    newest = int(dates[-1])
    all_pairs = [(dates[i - 1], dates[i]) for i in range(1, len(dates))]

    # First contact for this subscriber: everything available is undelivered.
    if cursor_timestamp is None:
        return None, all_pairs

    cursor_timestamp = int(cursor_timestamp)

    # Already at (or beyond - clocks only move forward) the newest snapshot.
    if cursor_timestamp >= newest:
        return None, []

    # History cleanup removed snapshots the cursor used to point at: we can
    # no longer prove what was and wasn't delivered, so say so explicitly and
    # resume from the oldest snapshot that is still on disk.
    if cursor_timestamp < oldest:
        logger.warning(
            f"RSS cursor {cursor_timestamp} for watch {watch.get('uuid')} is older than its oldest "
            f"surviving snapshot {oldest}; resyncing delivery position after history cleanup"
        )
        return ({'previous_timestamp': cursor_timestamp, 'oldest_timestamp': oldest}, all_pairs)

    # Normal case: every diff whose "to" snapshot is newer than the cursor.
    pending = [(from_ts, to_ts) for from_ts, to_ts in all_pairs if int(to_ts) > cursor_timestamp]
    return None, pending


def build_delivery_plan(datastore, cursor_store, subscriber, feed_key, watches, batch_limit):
    """Build the page of entries to deliver for one pull of one subscription.

    Reads every participating watch's cursor, plans its pending changes,
    merges the streams in real snapshot order and commits the page through
    the :class:`FeedCursorStore`. Returns the accepted item list (resync
    markers and changes) that the route should render, oldest first.
    """
    watches = list(watches)
    watch_uuids = [w['uuid'] for w in watches]
    cursors = cursor_store.snapshot_cursors(subscriber, feed_key, watch_uuids)

    planned = []
    for watch in watches:
        uuid = watch['uuid']
        cursor_ts = cursors.get(uuid)
        resync, pairs = plan_pending_for_watch(watch, cursor_ts)

        if resync is not None:
            planned.append({
                'kind': 'resync',
                'watch_uuid': uuid,
                'watch': watch,
                'previous_timestamp': resync['previous_timestamp'],
                'oldest_timestamp': resync['oldest_timestamp'],
                'sort_ts': resync['oldest_timestamp'],
            })

        for from_ts, to_ts in pairs:
            planned.append({
                'kind': 'change',
                'watch_uuid': uuid,
                'watch': watch,
                'from_timestamp': from_ts,
                'to_timestamp': to_ts,
                'sort_ts': int(to_ts),
            })

    # True snapshot order across watches; uuid/kind tie-breaks are only there
    # to make the order deterministic when timestamps coincide.
    planned.sort(key=lambda item: (item['sort_ts'], item['watch_uuid'], 0 if item['kind'] == 'resync' else 1))

    # Page size: rss_diff_length; 0 (or missing) means "no limit", matching
    # the legacy single-watch feed convention.
    if batch_limit and int(batch_limit) > 0:
        planned = planned[:int(batch_limit)]

    # Commit first, render second: a retry of this pull then finds the
    # positions already advanced and gets an empty feed instead of duplicates.
    return cursor_store.commit_plan(
        subscriber, feed_key, planned, alive_watch_uuids=watch_uuids
    )


def deliver_cursor_entries(fg, *, datastore, cursor_store, subscriber, feed_key,
                           watches, batch_limit, add_change_entry, marker_link_for):
    """Plan, commit and render one cursor-mode pull.

    Keeps the route views thin: the route supplies a straight-line
    ``add_change_entry(fg, watch, from_ts, to_ts)`` callback for its own
    entry style and a ``marker_link_for(watch)`` link builder; everything
    else (ordering, paging, resync markers, cursor advancement) is shared.
    Entries are appended oldest-first so the document follows snapshot order.
    """
    items = build_delivery_plan(
        datastore=datastore,
        cursor_store=cursor_store,
        subscriber=subscriber,
        feed_key=feed_key,
        watches=watches,
        batch_limit=batch_limit,
    )

    for item in items:
        if item['kind'] == 'resync':
            fe = fg.add_entry(order='append')
            add_resync_marker_entry(
                fe, item['watch'], item, link=marker_link_for(item['watch'])
            )
            add_watch_categories(fe, item['watch'], datastore)
        else:
            add_change_entry(fg, item['watch'], item['from_timestamp'], item['to_timestamp'])

    return items


def add_resync_marker_entry(fe, watch, item, link=None):
    """Render the history-gap marker as a normal, stable-GUID feed entry."""
    oldest = int(item['oldest_timestamp'])
    previous = int(item['previous_timestamp'])
    watch_label = watch.get('url', watch.get('uuid'))

    title = f"{watch_label} - delivery position reset after history cleanup"
    content = (
        f"History before snapshot {oldest} was cleaned up, so the previously stored read "
        f"position ({previous}) no longer exists. Delivery for this subscription resumes from "
        f"the oldest available snapshot ({oldest}); changes older than it cannot be replayed."
    )

    if link:
        fe.link(link=link)
    fe.title(title=title)
    fe.content(content=content, type='CDATA')
    fe.guid(resync_guid(watch['uuid'], oldest), permalink=False)
    fe.pubDate(datetime.datetime.fromtimestamp(oldest, tz=pytz.UTC))
