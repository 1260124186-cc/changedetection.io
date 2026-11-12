from flask import make_response, request


def _rss_response(fg):
    response = make_response(fg.rss_str())
    response.headers.set('Content-Type', 'application/rss+xml;charset=utf-8')
    return response


def _add_single_watch_change_entry(fg, datastore, notification_service, rss_content_format,
                                   watch_label, watch, timestamp_from, timestamp_to):
    """Render one snapshot diff in the single-watch feed style."""
    from . import RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT
    from ._util import (
        add_watch_categories,
        build_notification_context,
        get_rss_template,
        populate_feed_entry,
        render_notification,
    )

    n_body_template = get_rss_template(datastore, watch, rss_content_format,
                                       RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT)
    n_object = build_notification_context(watch, timestamp_from, timestamp_to,
                                          watch_label, n_body_template, rss_content_format)
    res = render_notification(n_object, notification_service, watch, datastore)

    guid = f"{watch['uuid']}/{timestamp_to}"
    fe = fg.add_entry(order='append')
    title_suffix = f"Change @ {res['original_context']['change_datetime']}"
    populate_feed_entry(fe, watch, res.get('body', ''), guid, timestamp_to,
                        link={'href': watch.open_link_override or watch.get('url')},
                        title_suffix=title_suffix)
    add_watch_categories(fe, watch, datastore)


def _watch_link(watch):
    return {'href': watch.open_link_override or watch.get('url')}


def serve_single_watch_feed(datastore, cursor_store, req, uuid):
    """Build the single-watch RSS response, in cursor mode or legacy mode."""
    import time

    from feedgen.feed import FeedGenerator
    from flask import Response
    from flask_babel import lazy_gettext as _l
    from loguru import logger

    from ...notification_service import NotificationService
    from ._util import get_watch_label, validate_rss_token
    from .cursor_store import InvalidSubscriberIdError, get_subscriber_id
    from .delivery import deliver_cursor_entries, feed_key_for_watch

    now = time.time()
    is_valid, error = validate_rss_token(datastore, req)
    if not is_valid:
        return error

    try:
        subscriber = get_subscriber_id(req)
    except InvalidSubscriberIdError as e:
        return Response(str(e), status=400, mimetype='text/plain')

    rss_content_format = datastore.data['settings']['application'].get('rss_content_format')

    if uuid == 'first':
        uuid = list(datastore.data['watching'].keys()).pop()
    watch = datastore.data['watching'].get(uuid)
    if not watch:
        return Response(_l("Watch with UUID %(uuid)s not found", uuid=uuid), status=404, mimetype='text/plain')

    dates = list(watch.history.keys())
    if len(dates) < 2 and subscriber is None:
        return Response(
            _l("Watch %(uuid)s does not have enough history snapshots to show changes (need at least 2)",
               uuid=uuid),
            status=400, mimetype='text/plain'
        )

    rss_diff_length = datastore.data['settings']['application'].get('rss_diff_length', 5)

    fg = FeedGenerator()
    watch_url = watch.get('url', '')
    watch_label = get_watch_label(datastore, watch)
    if watch_label != watch_url:
        feed_title = f'changedetection.io - {watch_label} ({watch_url})'
    else:
        feed_title = f'changedetection.io - {watch_url}'
    fg.title(feed_title)
    fg.description('Changes')
    fg.link(href='https://changedetection.io')

    notification_service = NotificationService(datastore=datastore, notification_q=False)

    # Cursor mode with fewer than two snapshots: nothing to deliver yet is a
    # normal, empty feed - the reader can keep polling this link.
    if len(dates) < 2:
        logger.debug(f"RSS Single watch (subscriber={subscriber}) empty, not enough snapshots")
        return _rss_response(fg)

    if subscriber is not None:
        # Cursor mode: oldest undelivered change first, one page per pull,
        # positions persisted per subscriber and independent of viewed state.
        deliver_cursor_entries(
            fg,
            datastore=datastore,
            cursor_store=cursor_store,
            subscriber=subscriber,
            feed_key=feed_key_for_watch(uuid),
            watches=[watch],
            batch_limit=rss_diff_length,
            add_change_entry=lambda target_fg, target_watch, ts_from, ts_to: _add_single_watch_change_entry(
                target_fg, datastore, notification_service, rss_content_format,
                watch_label, target_watch, ts_from, ts_to
            ),
            marker_link_for=_watch_link,
        )
        response = _rss_response(fg)
        logger.debug(f"RSS Single watch (subscriber={subscriber}) built in {time.time()-now:.2f}s")
        return response

    # Legacy mode: last rss_diff_length diffs, newest change first.
    max_possible_diffs = len(dates) - 1
    num_diffs = min(rss_diff_length, max_possible_diffs) if rss_diff_length > 0 else max_possible_diffs

    for i in range(num_diffs - 1, -1, -1):
        # i=0: compare dates[-2] to dates[-1] (most recent change)
        date_index_to = -(i + 1)
        date_index_from = -(i + 2)
        _add_single_watch_change_entry_legacy(
            fg, datastore, notification_service, watch, watch_label,
            dates, date_index_from, date_index_to
        )

    response = _rss_response(fg)
    logger.debug(f"RSS Single watch built in {time.time()-now:.2f}s")
    return response


def _add_single_watch_change_entry_legacy(fg, datastore, notification_service, watch,
                                          watch_label, dates, date_index_from, date_index_to):
    """Legacy single-watch entry: date-index based rendering, prepend (newest first)."""
    from . import RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT
    from ._util import (
        add_watch_categories,
        build_notification_context,
        get_rss_template,
        populate_feed_entry,
        render_notification,
    )

    timestamp_to = dates[date_index_to]
    timestamp_from = dates[date_index_from]
    rss_content_format = datastore.data['settings']['application'].get('rss_content_format')

    n_body_template = get_rss_template(datastore, watch, rss_content_format,
                                       RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT)
    n_object = build_notification_context(watch, timestamp_from, timestamp_to,
                                          watch_label, n_body_template, rss_content_format)
    res = render_notification(n_object, notification_service, watch, datastore,
                              date_index_from, date_index_to)

    guid = f"{watch['uuid']}/{timestamp_to}"
    fe = fg.add_entry()
    title_suffix = f"Change @ {res['original_context']['change_datetime']}"
    populate_feed_entry(fe, watch, res.get('body', ''), guid, timestamp_to,
                        link=_watch_link(watch), title_suffix=title_suffix)
    add_watch_categories(fe, watch, datastore)


def construct_single_watch_routes(rss_blueprint, datastore, cursor_store):
    """
    Construct RSS feed routes for single watches.

    Args:
        rss_blueprint: The Flask blueprint to add routes to
        datastore: The ChangeDetectionStore instance
        cursor_store: The per-subscriber FeedCursorStore instance
    """

    @rss_blueprint.route("/watch/<uuid_str:uuid>", methods=['GET'])
    def rss_single_watch(uuid):
        return serve_single_watch_feed(datastore, cursor_store, request, uuid)
