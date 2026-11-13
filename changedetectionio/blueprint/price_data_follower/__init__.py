
from flask import Blueprint, flash, redirect, url_for
from flask_babel import gettext
from loguru import logger
from changedetectionio.auth_decorator import login_optionally_required
from changedetectionio.store import ChangeDetectionStore
from changedetectionio import queuedWatchMetaData
from changedetectionio import worker_pool
from queue import PriorityQueue

PRICE_DATA_TRACK_ACCEPT = 'accepted'
PRICE_DATA_TRACK_REJECT = 'rejected'

def construct_blueprint(datastore: ChangeDetectionStore, update_q: PriorityQueue):

    price_data_follower_blueprint = Blueprint('price_data_follower', __name__)

    @price_data_follower_blueprint.route("/<uuid_str:uuid>/accept", methods=['POST'])
    @login_optionally_required
    def accept(uuid):
        # Carry the text-mode snapshots onto the restock timeline before switching mode,
        # instead of clearing history. On failure keep the old mode/history and let the user retry.
        try:
            datastore.migrate_watch_history(uuid, 'restock_diff')
        except Exception as e:
            logger.exception(f"History migration while following price data failed: {e}")
            flash(gettext("Could not switch to price/stock mode - history migration failed, the previous history was kept. See the log for details."), 'error')
            return redirect(url_for("watchlist.index"))

        datastore.data['watching'][uuid]['track_ldjson_price_data'] = PRICE_DATA_TRACK_ACCEPT
        datastore.data['watching'][uuid]['processor'] = 'restock_diff'
        datastore.data['watching'][uuid].commit()
        worker_pool.queue_item_async_safe(update_q, queuedWatchMetaData.PrioritizedItem(priority=1, item={'uuid': uuid}))
        return redirect(url_for("watchlist.index"))

    @price_data_follower_blueprint.route("/<uuid_str:uuid>/reject", methods=['POST'])
    @login_optionally_required
    def reject(uuid):
        datastore.data['watching'][uuid]['track_ldjson_price_data'] = PRICE_DATA_TRACK_REJECT
        datastore.data['watching'][uuid].commit()
        return redirect(url_for("watchlist.index"))


    return price_data_follower_blueprint


