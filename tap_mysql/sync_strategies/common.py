#!/usr/bin/env python3
# pylint: disable=missing-function-docstring,too-many-arguments,too-many-locals
import copy
import datetime
import math
import uuid

import pandas as pd
import singer
import time

from singer import metadata, utils, metrics, BatchMessage

from tap_mysql.stream_utils import get_key_properties, Constants, replace_select_with_count

LOGGER = singer.get_logger('tap_mysql')

TEMP_DATA_DIRECTORY = "/tmp/meltano_temp_data/"


def escape(string):
    if '`' in string:
        raise Exception(f"Can't escape identifier {string} because it contains a backtick")
    return '`' + string + '`'


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + '-' + table_name


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('database-name')


def generate_select_sql(catalog_entry, columns):
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = []

    for col_name in columns:
        # wrap the column name in "`"
        escaped_col = escape(col_name)

        # fetch the column type format from the json schema already built
        property_format = catalog_entry.schema.properties[col_name].format

        # if the column format is binary, fetch the values after removing any trailing
        # null bytes 0x00 and hexifying the column.
        if property_format == 'binary':
            escaped_columns.append(
                f'hex(trim(trailing CHAR(0x00) from {escaped_col})) as {escaped_col}')
        elif property_format == 'spatial':
            escaped_columns.append(
                f'ST_AsGeoJSON({escaped_col}) as {escaped_col}')
        else:
            escaped_columns.append(escaped_col)

    select_sql = f'SELECT {",".join(escaped_columns)} FROM {escaped_db}.{escaped_table}'

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql


def row_to_singer_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        property_format = catalog_entry.schema.properties[columns[idx]].format

        if property_format == 'date-time' or property_format == 'date':
            if isinstance(elem, datetime.datetime) or isinstance(elem, datetime.date):
                row_to_persist += (elem,)
            else:
                try:
                    datetime.datetime.fromisoformat(elem)
                except:
                    row_to_persist += (None,)
                else:
                    row_to_persist += (elem,)

        elif isinstance(elem, datetime.timedelta):
            if property_format == 'time':
                row_to_persist += (str(elem),) # this should convert time column into 'HH:MM:SS' formatted string
            else:
                epoch = datetime.datetime.utcfromtimestamp(0)
                timedelta_from_epoch = epoch + elem
                row_to_persist += (timedelta_from_epoch,)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem in (0, b'\x00'):
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bookmark_key in [non_whitelisted_bookmark_key for
                         non_whitelisted_bookmark_key in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
                         if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bookmark_key)


def get_result_size(query, params, cursor) -> int:
    count_query = replace_select_with_count(query)
    LOGGER.info(f"Row count is fetching: {count_query}")
    cursor.execute(count_query, params)
    result = cursor.fetchone()
    return result[0]


def result_generator(query, params, cursor):
    row_count = get_result_size(query, params, cursor)
    batch_size = Constants.QUERY_BATCH_SIZE

    batch_count = row_count / batch_size
    batch_count = math.ceil(batch_count)

    LOGGER.info(f"Row count: {str(row_count)} | Batch size: {str(batch_size)} | Batch count: {str(batch_count)}")

    fetched_batch = 0
    limit = batch_size
    while fetched_batch < batch_count:
        batch_query = query + f" LIMIT {fetched_batch * batch_size}, {limit};"

        LOGGER.info('Running %s', batch_query)

        cursor.execute(batch_query, params)

        rows = cursor.fetchall()
        LOGGER.info('Query executed')
        fetched_batch += 1
        yield rows


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params):
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    query_string = cursor.mogrify(select_sql, params)

    time_extracted = utils.now()

    database_name = get_database_name(catalog_entry)
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')
    record_message = None

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        for rows in result_generator(query=query_string, params=params, cursor=cursor):
            if len(rows) == 0:
                continue

            if Constants.FAST_SYNC:
                records = [
                    row_to_singer_record(
                        catalog_entry, stream_version, row, columns, time_extracted
                    ).asdict().get("record")
                    for row in rows
                ]
                df = pd.DataFrame.from_records(records)
                file_path = f"{TEMP_DATA_DIRECTORY}data_{str(uuid.uuid4())}.parquet"
                df.to_parquet(file_path, engine="pyarrow")

                message = BatchMessage(
                    stream=catalog_entry.stream,
                    filepath=file_path,
                    batch_size=len(rows),
                    time_extracted=time_extracted
                )
                singer.write_message(message)
                counter.increment(len(rows))
                if len(rows) > 0:
                    last_row = rows[-1]

                record_message = row_to_singer_record(
                    catalog_entry,
                    stream_version,
                    last_row,
                    columns,
                    time_extracted,
                )
            else:
                for row in rows:
                    counter.increment()
                    record_message = row_to_singer_record(catalog_entry,
                                                          stream_version,
                                                          row,
                                                          columns,
                                                          time_extracted)
                    singer.write_message(record_message)

        if record_message is not None:
            if replication_method in {'FULL_TABLE', 'LOG_BASED'}:
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'max_pk_values')

                if max_pk_values:
                    last_pk_fetched = {k:v for k, v in record_message.record.items()
                                       if k in key_properties}

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'last_pk_fetched',
                                                  last_pk_fetched)

            elif replication_method == 'INCREMENTAL':
                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key',
                                                  replication_key)

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key_value',
                                                  record_message.record[replication_key])

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
