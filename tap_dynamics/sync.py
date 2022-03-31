from datetime import datetime, timedelta
import pytz

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing
from odata import ODataError

from tap_dynamics.discover import discover

LOGGER = singer.get_logger()

MODIFIED_DATE_FIELD = "modifiedon"


def get_bookmark(state, stream_name, default):
    return state.get("bookmarks", {}).get(stream_name, default)


def write_bookmark(state, stream_name, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream_name] = value
    singer.write_state(state)


def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)


def sync_stream(service, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    max_modified = last_datetime

    ## TODO: add metrics?
    entitycls = service.entities[stream_name]

    if hasattr(entitycls, MODIFIED_DATE_FIELD):
        LOGGER.info(
            "{} - Syncing data since {}".format(stream.tap_stream_id, last_datetime)
        )

        query = _sync_stream_incremental(
            service,
            entitycls,
            singer.utils.strptime_with_tz(last_datetime),
        )
    else:
        LOGGER.info("{} - Syncing using full replication".format(stream.tap_stream_id))

        query = service.query(entitycls)

    schema = stream.schema.to_dict()

    count = 0
    with metrics.http_request_timer(stream.tap_stream_id):
        with metrics.record_counter(stream.tap_stream_id) as counter:
            for record in query:
                dict_record = {}
                for odata_prop in entitycls.__odata_schema__["properties"]:
                    prop_name = odata_prop["name"]
                    value = getattr(record, prop_name)
                    if isinstance(value, datetime):
                        value = singer.utils.strftime(value)
                    dict_record[prop_name] = value

                if MODIFIED_DATE_FIELD in dict_record:
                    if dict_record[MODIFIED_DATE_FIELD] > max_modified:
                        max_modified = dict_record[MODIFIED_DATE_FIELD]
                    else:
                        continue

                with Transformer() as transformer:
                    dict_record = transformer.transform(dict_record, schema, mdata)
                singer.write_record(stream.tap_stream_id, dict_record)
                counter.increment()

                count += 1
                if count % 5000 == 0:
                    write_bookmark(state, stream_name, max_modified)

    write_bookmark(state, stream_name, max_modified)


def _sync_stream_incremental(service, entitycls, start):
    base_query = service.query(entitycls)
    base_query = base_query.order_by(getattr(entitycls, MODIFIED_DATE_FIELD).asc())

    now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    delta = timedelta(days=30)

    f, t = start, start + delta

    while f < now:
        # loop_query = base_query.filter(getattr(entitycls, MODIFIED_DATE_FIELD) >= f)
        # loop_query = loop_query.filter(getattr(entitycls, MODIFIED_DATE_FIELD) <= t)

        yield from _sync_window(entitycls, base_query, f, t)

        f, t = t, t + delta


def _sync_window(entitycls, query, f, t):
    """
    Synchronizes data for the given entitycls in the window [f:t].

    Should the request for the window result in a response larger than 100MB,
    the window will be split in half, and new requests for the first and second
    half of the window will be tried instead. This process is done recursively,
    until one such window is found which results in a small enough response.
    If the window is shrunk to the size of f < 1ms < t, an Exception is raised.
    """
    if t - f < timedelta(milliseconds=1):
        raise Exception("unable to get data in a 1ms window")

    loop_query = query.filter(getattr(entitycls, MODIFIED_DATE_FIELD) >= f)
    loop_query = loop_query.filter(getattr(entitycls, MODIFIED_DATE_FIELD) <= t)

    try:
        yield from loop_query
    except ODataError as e:
        t2 = t - ((t - f) / 2)

        # Only shrink if the error indicates that the response would be too big
        if e.code != "0x80040216":
            raise

        # There's a 100MB limit to all responses from the OData API, which is
        # problematic due to there sometimes being binary data embedded in the
        # different resources we get.
        # It's not possible for us to _avoid_ getting these fields (at least not
        # in any nice generic way), so instead we shrink the window in which
        # we're getting data.
        # Basically, if the window results in too big of a response, we'll
        # shrink the window by half. Repeat recursively until the response is
        # small enough.
        yield from _sync_window(entitycls, query, f, t2)
        yield from _sync_window(entitycls, query, t2, t)


def update_current_stream(state, stream_name=None):
    set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(service, catalog, state, start_date):
    if not catalog:
        catalog = discover(service)
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    for stream in selected_streams:
        mdata = metadata.to_map(stream.metadata)
        update_current_stream(state, stream.tap_stream_id)
        sync_stream(service, catalog, state, start_date, stream, mdata)

    update_current_stream(state)
