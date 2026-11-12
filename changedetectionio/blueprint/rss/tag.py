from flask import make_response, request, url_for


def _select_tag_watches(datastore, tag_uuid):
    """Watches that belong to the tag and survive the muted filter."""
    tag_watches = []
    for uuid, watch in datastore.data['watching'].items():
        if tag_uuid not in watch.get('tags', []):
            continue
        if datastore.data['settings']['application'].get('rss_hide_muted_watches') and watch.get('notification_muted'):
            continue
        tag_watches.append(watch)
    return tag_watches


def _add_tag_change_entry(fg, datastore, notification_service, rss_content_format,
                          watch, timestamp_from, timestamp_to):
    """Render one snapshot diff as a tag-feed entry (legacy style)."""
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

    diff_link = {'href': url_for('ui.ui_diff.diff_history_page', uuid=watch['uuid'], _external=True)}
    watch_label = get_watch_label(datastore, watch)
    guid = generate_watch_guid(watch, timestamp_to)
    n_body_template = get_rss_template(datastore, watch, rss_content_format,
                                       RSS_TEMPLATE_HTML_DEFAULT, RSS_TEMPLATE_PLAINTEXT_DEFAULT)
    n_object = build_notification_context(watch, timestamp_from, timestamp_to,
                                          watch_label, n_body_template, rss_content_format)
    res = render_notification(n_object, notification_service, watch, datastore)

    fe = fg.add_entry(order='append')
    title_suffix = f"Change @ {res['original_context']['change_datetime']}"
    populate_feed_entry(fe, watch, res['body'], guid, timestamp_to, link=diff_link, title_suffix=title_suffix)
    add_watch_categories(fe, watch, datastore)


def _tag_diff_link(watch):
    return {'href': url_for('ui.ui_diff.diff_history_page', uuid=watch['uuid'], _external=True)}


def serve_tag_feed(datastore, cursor_store, req, tag_uuid):
    """Build the tag RSS response, in cursor mode or legacy mode."""
    from feedgen.feed import FeedGenerator

    from ...notification_service import NotificationService
    from ._util import validate_rss_token
    from .cursor_store import InvalidSubscriberIdError, get_subscriber_id
    from .delivery import deliver_cursor_entries, feed_key_for_tag

    is_valid, error = validate_rss_token(datastore, req)
    if not is_valid:
        return error

    try:
        subscriber = get_subscriber_id(req)
    except InvalidSubscriberIdError as e:
        from flask import Response
        return Response(str(e), status=400, mimetype='text/plain')

    rss_content_format = datastore.data['settings']['application'].get('rss_content_format')

    tag = datastore.data['settings']['application'].get('tags', {}).get(tag_uuid)
    if not tag:
        return f"Tag with UUID {tag_uuid} not found", 404

    fg = FeedGenerator()
    fg.title(f"changedetection.io - {tag.get('title', 'Unknown Tag')}")
    fg.description(f"Changes for watches tagged with {tag.get('title', 'Unknown Tag')}")
    fg.link(href='https://changedetection.io')
    notification_service = NotificationService(datastore=datastore, notification_q=False)

    tag_watches = _select_tag_watches(datastore, tag_uuid)

    if subscriber is not None:
        # Cursor mode: per-subscriber stream of this tag's changes in snapshot
        # order, unaffected by manual "mark viewed" in the UI.
        batch_limit = datastore.data['settings']['application'].get('rss_diff_length', 5)
        deliver_cursor_entries(
            fg,
            datastore=datastore,
            cursor_store=cursor_store,
            subscriber=subscriber,
            feed_key=feed_key_for_tag(tag_uuid),
            watches=tag_watches,
            batch_limit=batch_limit,
            add_change_entry=lambda target_fg, watch, ts_from, ts_to: _add_tag_change_entry(
                target_fg, datastore, notification_service, rss_content_format, watch, ts_from, ts_to
            ),
            marker_link_for=_tag_diff_link,
        )
        response = make_response(fg.rss_str())
        response.headers.set('Content-Type', 'application/rss+xml;charset=utf-8')
        return response

    # Legacy mode: one entry per unviewed watch with enough history.
    for watch in tag_watches:
        dates = list(watch.history.keys())
        if len(dates) < 2:
            continue
        if not watch.viewed:
            _add_tag_change_entry(fg, datastore, notification_service, rss_content_format,
                                  watch, dates[-2], dates[-1])

    response = make_response(fg.rss_str())
    response.headers.set('Content-Type', 'application/rss+xml;charset=utf-8')
    return response


def construct_tag_routes(rss_blueprint, datastore, cursor_store):
    """
    Construct RSS feed routes for tags.

    Args:
        rss_blueprint: The Flask blueprint to add routes to
        datastore: The ChangeDetectionStore instance
        cursor_store: The per-subscriber FeedCursorStore instance
    """

    @rss_blueprint.route("/tag/<uuid_str:tag_uuid>", methods=['GET'])
    def rss_tag_feed(tag_uuid):
        return serve_tag_feed(datastore, cursor_store, request, tag_uuid)
