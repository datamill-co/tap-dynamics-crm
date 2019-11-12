from datetime import datetime

import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

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
    query = service.query(entitycls)

    if hasattr(entitycls, MODIFIED_DATE_FIELD):
        LOGGER.info(
            "{} - Syncing data since {}".format(stream.tap_stream_id, last_datetime)
        )
        query = query.filter(
            getattr(entitycls, MODIFIED_DATE_FIELD)
            >= singer.utils.strptime_with_tz(last_datetime)
        ).order_by(getattr(entitycls, MODIFIED_DATE_FIELD).asc())
    else:
        LOGGER.info("{} - Syncing using full replication".format(stream.tap_stream_id))

    schema = stream.schema.to_dict()
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for record in query:
            dict_record = {}
            for odata_prop in entitycls.__odata_schema__["properties"]:
                prop_name = odata_prop["name"]
                value = getattr(record, prop_name)
                if isinstance(value, datetime):
                    value = singer.utils.strftime(value)
                dict_record[prop_name] = value

            if (
                MODIFIED_DATE_FIELD in dict_record
                and dict_record[MODIFIED_DATE_FIELD] > max_modified
            ):
                max_modified = dict_record[MODIFIED_DATE_FIELD]

            with Transformer() as transformer:
                dict_record = transformer.transform(dict_record, schema, mdata)
            singer.write_record(stream.tap_stream_id, dict_record)
            counter.increment()

    write_bookmark(state, stream_name, max_modified)


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
