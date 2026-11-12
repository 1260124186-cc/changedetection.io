
from flask import Blueprint

from changedetectionio.store import ChangeDetectionStore

from . import main_feed, single_watch
from . import tag as tag_routes
from .cursor_store import FeedCursorStore


def construct_blueprint(datastore: ChangeDetectionStore):
    """
    Construct and configure the RSS blueprint with all routes.

    Args:
        datastore: The ChangeDetectionStore instance

    Returns:
        The configured Flask blueprint
    """
    rss_blueprint = Blueprint('rss', __name__)

    # Per-subscriber delivery positions, persisted in rss-cursors.json inside
    # the datastore. One store for the whole app process; lazily loaded.
    cursor_store = FeedCursorStore(datastore.datastore_path)

    # Register all route modules
    main_feed.construct_main_feed_routes(rss_blueprint, datastore, cursor_store)
    single_watch.construct_single_watch_routes(rss_blueprint, datastore, cursor_store)
    tag_routes.construct_tag_routes(rss_blueprint, datastore, cursor_store)

    return rss_blueprint
