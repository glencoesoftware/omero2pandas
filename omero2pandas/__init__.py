# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import collections
import logging
import os
import sys

import pandas
import omero
import omero.gateway
from tqdm.auto import tqdm

from omero2pandas.connect import OMEROConnection
from omero2pandas.upload import create_table

logging.basicConfig(
    format="%(asctime)s %(levelname)-7s [%(name)16s] %(message)s",
    stream=sys.stdout)

LOGGER = logging.getLogger(__name__)


def get_table_size(file_id=None, annotation_id=None, omero_connector=None,
                   server=None, port=4064, username=None, password=None):
    """
    Gets table dimensions from the server.
    Supply either a file or annotation ID.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param file_id: ID of an OriginalFile object containing a table
    :param annotation_id: ID of a FileAnnotation object containing a table
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :return: Tuple of ints - (table row count, table column count)
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    with OMEROConnection(server=server, username=username, password=password,
                         port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()
        data_table = _get_table(conn, object_type, object_id)
        # Fetch columns
        return data_table.getNumberOfRows(), len(data_table.getHeaders())


def get_table_columns(file_id=None, annotation_id=None,
                      omero_connector=None, server=None, port=4064,
                      username=None, password=None):
    """
    Gets table column names from the server.
    Supply either a file or annotation ID.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param file_id: ID of an OriginalFile object containing a table
    :param annotation_id: ID of a FileAnnotation object containing a table
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :return: List of column names from the table
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    with OMEROConnection(server=server, username=username, password=password,
                         port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()

        data_table = _get_table(conn, object_type, object_id)
        # Fetch columns
        heads = data_table.getHeaders()
    return [col.name for col in heads]


def read_table(file_id=None, annotation_id=None, column_names=(), rows=None,
               chunk_size=1000, omero_connector=None, server=None, port=4064,
               username=None, password=None):
    """
    Gets table data from the server.
    Supply either a file or annotation ID.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param file_id: ID of an OriginalFile object containing a table
    :param annotation_id: ID of a FileAnnotation object containing a table
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :param chunk_size: Number of rows to load from the server in a single
    operation (default 1000)
    :param rows: Iterable of integers specifying row numbers to load.
    Default None = load all rows.
    :param column_names: Iterable of column name strings to load.
    Default None = load all columns.
    :return: pandas.DataFrame object containing requested data
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    with OMEROConnection(server=server, username=username, password=password,
                         port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()

        data_table = _get_table(conn, object_type, object_id)
        # Validate columns
        heads = data_table.getHeaders()
        if len(column_names):
            target_cols = [i for i, col in enumerate(heads) if
                           col.name in column_names]
            if len(target_cols) != len(column_names):
                present_cols = [col.name for col in heads if
                                col.name in column_names]
                missing = set(column_names) - set(present_cols)
                LOGGER.warning(f"Table {object_id} is missing "
                               f"requested column(s): {missing}")
        else:
            target_cols = range(len(heads))
        # Determine requested rows
        if rows is None:
            num_rows = data_table.getNumberOfRows()
        else:
            rows = list(rows)
            num_rows = len(rows)
        # Construct Pandas DataFrame
        LOGGER.info(f"Loading table {object_id} from "
                    f"OMERO ({num_rows} records).")
        data_buffer = collections.defaultdict(list)
        # Row numbers are returned even if they don't exist,
        # so we handle these differently
        index_buffer = []
        bar_fmt = '{desc}: {percentage:3.0f}%|{bar}| ' \
                  '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}'
        chunk_iter = tqdm(desc="Downloading table from OMERO", total=num_rows,
                          bar_format=bar_fmt)
        # Download data
        for start in range(0, num_rows, chunk_size):
            if rows is None:
                data = data_table.read(target_cols, start, start + chunk_size)
            else:
                data = data_table.slice(target_cols,
                                        rows[start:start + chunk_size])
            for col in data.columns:
                data_buffer[col.name] += col.values
            index_buffer += data.rowNumbers
            chunk_iter.update(len(data.columns[0].values))
        chunk_iter.close()
    df = pandas.DataFrame.from_dict(data_buffer)
    df.index = index_buffer[0: len(df)]
    return df


def upload_table(dataframe, table_name, parent_id, parent_type='Image',
                 chunk_size=1000, omero_connector=None, server=None,
                 port=4064, username=None, password=None):
    """
    Upload a pandas dataframe to a new OMERO table.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param dataframe: Pandas dataframe to upload to OMERO
    :param table_name: Name for the table on OMERO
    :param parent_id: Object ID to attach the table to as an annotation.
    :param parent_type: Object type to attach to.
    One of: Image, Dataset, Plate, Well
    :param chunk_size: Rows to transmit to the server in a single operation
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :return: File annotation ID of the new table
    """
    with OMEROConnection(server=server, username=username, password=password,
                         port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()
        conn.SERVICE_OPTS.setOmeroGroup('-1')
        ann_id = create_table(dataframe, table_name, parent_id, parent_type,
                              conn, chunk_size)
        if ann_id is None:
            LOGGER.warning("Failed to create OMERO table")
        return ann_id


def download_table(target_path, file_id=None, annotation_id=None,
                   column_names=(), rows=None, chunk_size=1000,
                   omero_connector=None, server=None, port=4064,
                   username=None, password=None):
    """
    Downloads table data into a CSV file.
    Supply either a file or annotation ID.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param target_path: Path to the csv file where data will be saved.
    :param file_id: ID of an OriginalFile object containing a table
    :param annotation_id: ID of a FileAnnotation object containing a table
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :param chunk_size: Number of rows to load from the server in a single
    operation (default 1000)
    :param rows: Iterable of integers specifying row numbers to load.
    Default None = load all rows.
    :param column_names: Iterable of column name strings to load.
    Default None = load all columns.
    :return: pandas.DataFrame object containing requested data
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    assert not os.path.exists(target_path), \
        f"Target file {target_path} already exists"

    with OMEROConnection(server=server, username=username, password=password,
                         port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()

        data_table = _get_table(conn, object_type, object_id)

        # Validate columns
        heads = data_table.getHeaders()
        if len(column_names):
            target_cols = [i for i, col in enumerate(heads) if
                           col.name in column_names]
            if len(target_cols) != len(column_names):
                present_cols = [col.name for col in heads if
                                col.name in column_names]
                missing = set(column_names) - set(present_cols)
                LOGGER.warning(f"Table {object_id} is missing "
                               f"requested column(s): {missing}")
        else:
            target_cols = range(len(heads))
        # Determine requested rows
        if rows is None:
            num_rows = data_table.getNumberOfRows()
        else:
            rows = list(rows)
            num_rows = len(rows)
        # Construct Pandas DataFrame
        LOGGER.info(f"Downloading table {object_id} from "
                    f"OMERO ({num_rows} records).")
        os.makedirs(os.path.dirname(os.path.abspath(target_path)),
                    exist_ok=True)
        bar_fmt = '{desc}: {percentage:3.0f}%|{bar}| ' \
                  '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}'
        chunk_iter = tqdm(desc="Downloading table from OMERO", total=num_rows,
                          bar_format=bar_fmt)
        # Download data
        for start in range(0, num_rows, chunk_size):
            data_buffer = {}
            if rows is None:
                data = data_table.read(target_cols, start, start + chunk_size)
            else:
                data = data_table.slice(target_cols,
                                        rows[start:start + chunk_size])
            for col in data.columns:
                data_buffer[col.name] = col.values
            df = pandas.DataFrame.from_dict(data_buffer)
            df.index = data.rowNumbers[0: len(df)]
            df.to_csv(target_path, index=True, mode='a',
                      header=not os.path.exists(target_path))

            chunk_iter.update(len(data.columns[0].values))
        chunk_iter.close()
    LOGGER.info(f"Download complete, saved to {target_path}")


def _get_table(conn, object_type, object_id):
    """
    Loads an OMERO.table remotely
    :param conn: BlitzGateway object with a live OMERO session.
    :param object_type: Type of object referenced by the provided ID,
    should be a FileAnnotation or OriginalFile
    :param object_id: OMERO ID of the table object. Tables are referenced
    by both a FileAnnotation and OriginalFile ID
    :return: Activated OMERO.table wrapper
    """
    orig_group = conn.SERVICE_OPTS.getOmeroGroup()
    conn.SERVICE_OPTS.setOmeroGroup('-1')
    # Fetch the object from OMERO
    target = conn.getObject(object_type, object_id)
    assert target is not None, f"{object_type} with ID" \
                               f" {object_id} not found"
    # Get the file object containing the table.
    if isinstance(target, omero.gateway.FileAnnotationWrapper):
        orig_file = target.file
    elif isinstance(target, omero.gateway.OriginalFileWrapper):
        orig_file = target._obj
    else:
        raise NotImplementedError(
            f"OMERO object of type {type(target)} is not supported")
    # Load the table
    resources = conn.c.sf.sharedResources()
    data_table = resources.openTable(orig_file, _ctx=conn.SERVICE_OPTS)
    conn.SERVICE_OPTS.setOmeroGroup(orig_group)
    return data_table


def _validate_requested_object(file_id, annotation_id):
    # Validate that the requested ID is sensible.
    if file_id and annotation_id:
        raise ValueError("Supply either a File or Annotation ID, not both")
    elif file_id is not None:
        object_id = file_id
        object_type = 'OriginalFile'
    elif annotation_id is not None:
        object_id = annotation_id
        object_type = 'FileAnnotation'
    else:
        raise ValueError("OMERO.table File or Annotation ID must be supplied.")

    if isinstance(object_id, str):
        assert object_id.isnumeric(), "Object ID should be a number."
        object_id = int(object_id)
    elif not isinstance(object_id, int):
        raise ValueError("Object ID should be an integer.")
    return object_id, object_type


def connect_to_omero(client=None, server=None, port=4064,
                     username=None, password=None, session_key=None,
                     allow_token=True, interactive=True):
    """
    Connect to OMERO and return an OMEROConnection object.
    :param client: An existing omero.client object to be used instead of
    creating a new connection.
    :param server: server address to connect to
    :param port: port number of server (default 4064)
    :param username: username for server login
    :param password: password for server login
    :param session_key: key to join an existing session,
    supersedes username/password
    :param allow_token: True/False Search for omero_user_token before trying
    to use credentials. Default True.
    :param interactive: Prompt user for missing login details. Default True.
    :return: OMEROConnection object wrapping a client and Blitz Gateway object,
    with automatic session management and cleanup.
    """
    connector = OMEROConnection(server=server, port=port,
                                session_key=session_key, username=username,
                                password=password, client=client,
                                allow_token=allow_token)
    connector.connect(interactive=interactive)
    return connector
