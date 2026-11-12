from flask import make_response, redirect, request, url_for


def _resolve_tag_filter(datastore, req):
    """Resolve the optional ?tag= title/UUID filter to a tag UUID or ''."""
    limit_tag = req.args.get('tag', '').lower().strip()
    for uuid, tag in datastore.data['settings']['application'].get('tags', {}).items():
        if limit_tag == tag.get('title', '').lower().strip():
            limit_tag = uuid
    return limit_tag


def _select_watches(datastore, limit_tag):
    """Watches that participate in the main feed, sorted by last_changed."""
    sorted_watches = []
    for uuid, watch in datastore.data['watching'].items():
        if datastore.data['settings']['application'].get('rss_hide_muted_watches') and watch.get('notification_muted'):
            continue
        if limit_tag and limit_tag not in watch['tags']:
            continue
        sorted_watches.append(watch)

    sorted_watches.sort(key=lambda x: x.last_changed, reverse=False)
    return sorted_watches


def _add_main_change_entry(fg, datastore, notification_service, rss_content_format,
                           watch, timestamp_from, timestamp_to):
    """Render one snapshot diff as a main-feed entry (legacy style)."""
    from . import RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT
    from ._util import (
        add_watch_categories,
        build_notification_context,
        generate_watch_guid,
        get_rss_template,
        get_watch_label,
        populate_feed_entry,
        render_notification,
    )

    watch_label = get_watch_label(datastore, watch)
    guid = generate_watch_guid(watch, timestamp_to)
    # Because we are called via whatever web server, flask should figure out the right path
    diff_link = {'href': url_for('ui.ui_diff.diff_history_page', uuid=watch['uuid'], _external=True)}

    n_body_template = get_rss_template(datastore, watch, rss_content_format,
                                       RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT)
    n_object = build_notification_context(watch, timestamp_from, timestamp_to,
                                          watch_label, n_body_template, rss_content_format)
    res = render_notification(n_object, notification_service, watch, datastore)

    fe = fg.add_entry(order='append')
    populate_feed_entry(fe, watch, res['body'], guid, timestamp_to, link=diff_link)
    fe.title(title=watch_label)  # Override title to not include suffix
    add_watch_categories(fe, watch, datastore)


def _diff_link_for(watch):
    return {'href': url_for('ui.ui_diff.diff_history_page', uuid=watch['uuid'], _external=True)}


def serve_main_feed(datastore, cursor_store, req):
    """Build the main RSS response, in cursor mode or legacy mode."""
    import time

    from feedgen.feed import FeedGenerator
    from loguru import logger

    from ...notification_service import NotificationService
    from ._util import validate_rss_token
    from .cursor_store import InvalidSubscriberIdError, get_subscriber_id
    from .delivery import FEED_KEY_MAIN, deliver_cursor_entries

    is_valid, error = validate_rss_token(datastore, req)
    if not is_valid:
        return error

    now = time.time()
    try:
        subscriber = get_subscriber_id(req)
    except InvalidSubscriberIdError as e:
        from flask import Response
        return Response(str(e), status=400, mimetype='text/plain')

    rss_content_format = datastore.data['settings']['application'].get('rss_content_format')
    sorted_watches = _select_watches(datastore, _resolve_tag_filter(datastore, req))

    fg = FeedGenerator()
    fg.title('changedetection.io')
    fg.description('Feed description')
    fg.link(href='https://changedetection.io')
    notification_service = NotificationService(datastore=datastore, notification_q=False)

    if subscriber is not None:
        # Cursor mode: deliver only not-yet-delivered changes in real snapshot
        # order, one independent page per subscriber.
        batch_limit = datastore.data['settings']['application'].get('rss_diff_length', 5)
        deliver_cursor_entries(
            fg,
            datastore=datastore,
            cursor_store=cursor_store,
            subscriber=subscriber,
            feed_key=FEED_KEY_MAIN,
            watches=sorted_watches,
            batch_limit=batch_limit,
            add_change_entry=lambda target_fg, watch, ts_from, ts_to: _add_main_change_entry(
                target_fg, datastore, notification_service, rss_content_format, watch, ts_from, ts_to
            ),
            marker_link_for=_diff_link_for,
        )
        response = make_response(fg.rss_str())
        response.headers.set('Content-Type', 'application/rss+xml;charset=utf-8')
        logger.trace(f"RSS (subscriber={subscriber}) generated in {time.time() - now:.3f}s")
        return response

    # Legacy mode: one entry per unviewed watch, its latest change.
    for watch in sorted_watches:
        dates = list(watch.history.keys())
        # Re #521 - less than 2 snapshots means no change was ever detected.
        if len(dates) < 2:
            continue
        if not watch.viewed:
            _add_main_change_entry(fg, datastore, notification_service, rss_content_format,
                                   watch, dates[-2], dates[-1])

    response = make_response(fg.rss_str())
    response.headers.set('Content-Type', 'application/rss+xml;charset=utf-8')
    logger.trace(f"RSS generated in {time.time() - now:.3f}s")
    return response


def construct_main_feed_routes(rss_blueprint, datastore, cursor_store):
    """
    Construct the main RSS feed routes.

    Args:
        rss_blueprint: The Flask blueprint to add routes to
        datastore: The ChangeDetectionStore instance
        cursor_store: The per-subscriber FeedCursorStore instance
    """

    # Some RSS reader situations ended up with rss/ (forward slash after RSS) due
    # to some earlier blueprint rerouting work, it should goto feed.
    @rss_blueprint.route("/", methods=['GET'])
    def extraslash():
        return redirect(url_for('rss.feed'))

    @rss_blueprint.route("", methods=['GET'])
    def feed():
        return serve_main_feed(datastore, cursor_store, request)
