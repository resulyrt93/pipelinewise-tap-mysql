#!/usr/bin/env python3
# pylint: disable=too-many-locals,missing-function-docstring

import singer

from singer import metadata

from tap_mysql.sync_strategies import binlog
from tap_mysql.sync_strategies import common

from tap_mysql.connection import connect_with_backoff

LOGGER = singer.get_logger('tap_mysql')


def generate_bookmark_keys(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')

    base_bookmark_keys = {'last_pk_fetched', 'max_pk_values', 'version', 'initial_full_table_complete'}

    if replication_method == 'FULL_TABLE':
        bookmark_keys = base_bookmark_keys
    else:
        bookmark_keys = base_bookmark_keys.union(binlog.BOOKMARK_KEYS)

    return bookmark_keys


def pks_are_auto_incrementing(mysql_conn, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if not key_properties:
        return False

    sql = """SELECT 1
               FROM information_schema.columns
              WHERE table_schema = '{}'
                AND table_name = '{}'
                AND column_name = '{}'
                AND extra LIKE '%auto_increment%'
    """

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            for primary_key in key_properties:
                cur.execute(sql.format(database_name,
                                       catalog_entry.table,
                                       primary_key))

                result = cur.fetchone()

                if not result:
                    return False

    return True


def get_max_pk_values(cursor, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    escaped_db = common.escape(database_name)
    escaped_table = common.escape(catalog_entry.table)

    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    sql = """SELECT {}
               FROM {}.{}
              ORDER BY {}
              LIMIT 1
    """

    select_column_clause = ", ".join(escaped_columns)
    order_column_clause = ", ".join([primary_key + " DESC" for primary_key in escaped_columns])

    cursor.execute(sql.format(select_column_clause,
                              escaped_db,
                              escaped_table,
                              order_column_clause))
    result = cursor.fetchone()

    if result:
        max_pk_values = dict(zip(key_properties, result))
    else:
        max_pk_values = {}

    return max_pk_values


def calculate_pk_windows(max_value: int, constant: int = 10000):
    result = [0]
    for i in range(constant, max_value, constant):
        result.append(i)
    if max_value not in result:
        result.append(max_value)
    return result


def generate_pk_clauses(catalog_entry, state):
    key_properties = common.get_key_properties(catalog_entry)

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    selected_key_property = key_properties[0]
    escaped_key_property = common.escape(selected_key_property)
    windows = calculate_pk_windows(max_pk_values[selected_key_property], 10**6)
    clauses = []
    for i in range(len(windows) - 1):
        left_window = windows[i]
        right_window = windows[i+1]

        sql = f' WHERE {escaped_key_property} > {str(left_window)} AND {escaped_key_property} <= {str(right_window)} ORDER BY {escaped_key_property} ASC'
        clauses.append(sql)

    return clauses


def sync_table(mysql_conn, catalog_entry, state, columns, stream_version):
    common.whitelist_bookmark_keys(generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state)

    bookmark = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id, {})
    version_exists = 'version' in bookmark

    initial_full_table_complete = singer.get_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'initial_full_table_complete')

    state_version = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'version')

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    key_props_are_auto_incrementing = pks_are_auto_incrementing(mysql_conn, catalog_entry)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)

            pk_clauses = ['']

            if key_props_are_auto_incrementing:
                LOGGER.info("Detected auto-incrementing primary key(s) - will replicate incrementally")
                max_pk_values = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'max_pk_values') or get_max_pk_values(cur, catalog_entry)

                if not max_pk_values:
                    LOGGER.info("No max value for auto-incrementing PK found for table %s", catalog_entry.table)
                else:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'max_pk_values',
                                                  max_pk_values)

                    pk_clauses = generate_pk_clauses(catalog_entry, state)

            params = {}

            for pk_clause in pk_clauses:
                query = select_sql + pk_clause
                # pylint:disable=duplicate-code
                common.sync_query(cur,
                                  catalog_entry,
                                  state,
                                  query,
                                  columns,
                                  stream_version,
                                  params)

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    singer.write_message(activate_version_message)
