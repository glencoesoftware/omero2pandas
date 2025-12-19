# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import collections
from importlib.util import find_spec
import logging
import os
from typing import Iterable

import pandas
import omero
import omero.gateway
from omero.rtypes import unwrap
from tqdm.auto import tqdm

from omero2pandas.connect import get_connection
from omero2pandas.io_tools import get_annotation_reader, infer_compression
from omero2pandas.upload import create_table
if find_spec("tiledb"):
    from omero2pandas.remote import create_remote_table
else:
    create_remote_table = None

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

    with get_connection(server=server, username=username, password=password,
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

    with get_connection(server=server, username=username, password=password,
                        port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()

        data_table = _get_table(conn, object_type, object_id)
        # Fetch columns
        heads = data_table.getHeaders()
    return [col.name for col in heads]


def read_table(file_id=None, annotation_id=None, column_names=(), rows=None,
               chunk_size=1000, omero_connector=None, server=None, port=4064,
               username=None, password=None, query=None, variables=None):
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
    :param query: String containing the PyTables query which would return a
    subset of rows from the table. Only rows which pass this query will be
    returned. Cannot be used with the 'rows' parameter.
    :param variables: Dictionary containing variables to map onto the query
    string.
    :return: pandas.DataFrame object containing requested data
    """
    if rows is not None and query is not None:
        raise ValueError("Running a query supersedes the rows argument. "
                         "Please only supply one.")
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    with get_connection(server=server, username=username, password=password,
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
        if query is not None:
            if variables is None:
                variables = {}
            rows = data_table.getWhereList(condition=query,
                                           variables=variables,
                                           start=0, stop=-1, step=1)
            num_rows = len(rows)
        elif rows is None:
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
                end = min(start + chunk_size, num_rows)
                data = data_table.read(target_cols, start, end)
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


def upload_table(source, table_name, parent_id=None, parent_type='Image',
                 links=None, chunk_size=None, omero_connector=None,
                 server=None, port=4064, username=None, password=None,
                 local_path=None, remote_path=None, prefix=""):
    """
    Upload a pandas dataframe to a new OMERO table.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param source: Pandas dataframe or CSV file path to upload to OMERO
    :param table_name: Name for the table on OMERO
    :param parent_id: Object ID to attach the table to as an annotation.
    :param parent_type: Object type to attach to.
    One of: Image, Dataset, Project, Well, Plate, Screen, Roi
    :param links: List of (Type, ID) tuples specifying objects to
    link the table to.
    :param chunk_size: Rows to transmit to the server in a single operation.
    Default: Automatically choose a size
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param local_path: [TileDB only], construct table at this file path and
                       register remotely
    :param remote_path: [TileDB only], mapping for local_path on the server
                        (if different from local system)
    :param prefix: [TileDB only], API prefix for your OMERO server,
                   relative to server URL. Use this if your OMERO server
                   is not at the top-level URL of the server.
                   e.g. for my.omero.server/custom_omero
                   supply prefix="custom_omero"
    :param password: Password for server login
    :return: File Annotation ID of the new table
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError(f"Invalid table name: '{table_name}'")
    # Coerce inputs to the links list input format
    links = links or []
    if (len(links) == 2 and
            isinstance(links[0], str) and isinstance(links[1], int)):
        # Someone forgot to nest their tuples, let's fix that
        links = [links]
    elif isinstance(links, tuple):
        # Make sure it's mutable
        links = list(links)
    if parent_id is not None:
        if (parent_type, parent_id) not in links:
            links.append((parent_type, parent_id))
    if not links:
        raise ValueError("No OMERO objects to link the table to")
    elif not isinstance(links, Iterable):
        raise ValueError(f"Links should be an iterable list of "
                         f"type/id pairs, not {type(links)}")
    with get_connection(server=server, username=username, password=password,
                        port=port, client=omero_connector) as connector:
        if local_path or remote_path:
            if not create_remote_table:
                raise ValueError("Remote table support is not installed")
            ann_id = create_remote_table(source, table_name, local_path,
                                         remote_path=remote_path,
                                         links=links,
                                         chunk_size=chunk_size,
                                         connector=connector,
                                         prefix=prefix)
        else:
            conn = connector.get_gateway()
            conn.SERVICE_OPTS.setOmeroGroup('-1')
            ann_id = create_table(source, table_name, links, conn, chunk_size)
        if ann_id is None:
            LOGGER.warning("Failed to create OMERO table")
        return ann_id


def download_table(target_path, file_id=None, annotation_id=None,
                   column_names=(), rows=None, chunk_size=1000,
                   omero_connector=None, server=None, port=4064,
                   username=None, password=None, query=None, variables=None):
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
    :param query: String containing the PyTables query which would return a
    subset of rows from the table. Only rows which pass this query will be
    returned. Cannot be used with the 'rows' parameter.
    :param variables: Dictionary containing variables to map onto the query
    string.
    """
    if rows is not None and query is not None:
        raise ValueError("Running a query supersedes the rows argument. "
                         "Please only supply one.")
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    assert not os.path.exists(target_path), \
        f"Target file {target_path} already exists"

    with get_connection(server=server, username=username, password=password,
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
        if query is not None:
            if variables is None:
                variables = {}
            rows = data_table.getWhereList(condition=query,
                                           variables=variables,
                                           start=0, stop=-1, step=1)
            num_rows = len(rows)
        elif rows is None:
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
                end = min(start + chunk_size, num_rows)
                data = data_table.read(target_cols, start, end)
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


def read_csv(file_id=None, annotation_id=None, column_names=None,
             chunk_size=1048576, omero_connector=None, server=None, port=4064,
             username=None, password=None, **kwargs):
    """
    Read a csv or csv.gz file stored as an OMERO OriginalFile/FileAnnotation
    into a pandas dataframe.
    Supply either a file or annotation ID.
    Convenience method for scenarios where data was uploaded as a raw CSV
    rather than an OMERO.tables object.
    Additional keyword arguments will be forwarded to the pandas.read_csv
    method
    :param file_id: ID of the OriginalFile to load
    :param annotation_id: ID of the FileAnnotation to load
    :param column_names: Optional list of column names to return
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :param chunk_size: BYTES to download in a single call (default 1024^2)
    :return: pandas.DataFrame
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)
    if "usecols" in kwargs:
        raise ValueError(
            "Provide 'column_names' for column selection, not 'usecols'")

    with get_connection(server=server, username=username, password=password,
                        port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()
        orig_file = _get_original_file(conn, object_type, object_id)
        file_id = unwrap(orig_file.id)
        file_name = unwrap(orig_file.name)
        file_mimetype = unwrap(orig_file.mimetype)
        if "compression" not in kwargs:
            compression = infer_compression(file_mimetype, file_name)

        # Check that the OriginalFile has the expected mimetype

        LOGGER.info(f"Reading file {file_id} of "
                    f"mimetype '{file_mimetype}' from OMERO")
        bar_fmt = '{desc}: {percentage:3.0f}%|{bar}| ' \
                  '{n_fmt}/{total_fmt}, {elapsed} {postfix}'
        chunk_iter = tqdm(desc="Reading CSV from OMERO",
                          bar_format=bar_fmt, unit_scale=True)
        with get_annotation_reader(conn, file_id,
                                   chunk_size, reporter=chunk_iter) as reader:
            df = pandas.read_csv(reader,
                                 compression=compression,
                                 usecols=column_names, **kwargs)
        chunk_iter.close()
    return df


def download_csv(target_path, file_id=None, annotation_id=None,
                 chunk_size=1048576, check_type=True, omero_connector=None,
                 server=None, port=4064, username=None, password=None):
    """
    Downloads a CSV file stored as a csv or csv.gz file rather than an
    OMERO.table.
    Supply either a file or annotation ID.
    For the connection, supply either an active client object or server
    credentials (not both!). If neither are provided the program will search
    for an OMERO user token on the system.
    :param target_path: Path to the csv file where data will be saved.
    :param file_id: ID of an OriginalFile object
    :param annotation_id: ID of a FileAnnotation object
    :param omero_connector: OMERO.client object which is already connected
    to a server. Supersedes any other connection details.
    :param server: Address of the server
    :param port: Port the server runs on (default 4064)
    :param username: Username for server login
    :param password: Password for server login
    :param chunk_size: BYTES to download in a single call (default 1024^2)
    :param check_type: [Boolean] Whether to check that the target file is
    actually a CSV
    """
    object_id, object_type = _validate_requested_object(
        file_id=file_id, annotation_id=annotation_id)

    assert not os.path.exists(target_path), \
        f"Target file {target_path} already exists"

    with get_connection(server=server, username=username, password=password,
                        port=port, client=omero_connector) as connector:
        conn = connector.get_gateway()

        orig_file = _get_original_file(conn, object_type, object_id)
        file_id = unwrap(orig_file.id)
        file_name = unwrap(orig_file.name)
        file_mimetype = unwrap(orig_file.mimetype)
        if check_type:
            infer_compression(file_mimetype, file_name)

        LOGGER.info(f"Downloading file {file_id} of "
                    f"mimetype '{file_mimetype}' from OMERO")
        bar_fmt = '{desc}: {percentage:3.0f}%|{bar}| ' \
                  '{n_fmt}/{total_fmt}B, {elapsed} {postfix}'
        chunk_iter = tqdm(desc="Downloading CSV from OMERO",
                          bar_format=bar_fmt, unit_scale=True)
        with get_annotation_reader(conn, file_id,
                                   chunk_size, reporter=chunk_iter) as reader:
            with open(target_path, "wb") as filehandle:
                for chunk in reader:
                    filehandle.write(chunk)
        chunk_iter.close()
    LOGGER.info(f"Download complete, saved to {target_path}")


def _get_original_file(conn, object_type, object_id):
    if object_type not in ("FileAnnotation", "OriginalFile"):
        raise ValueError(f"Unsupported type '{object_type}'")
    # Fetch the object from OMERO
    if object_type == "FileAnnotation":
        params = omero.sys.ParametersI()
        params.addId(object_id)
        target = conn.getQueryService().findByQuery(
            "SELECT fa.file from FileAnnotation fa where fa.id = :id",
            params, {"omero.group": "-1"})
    elif object_type == "OriginalFile":
        target = conn.getQueryService().find(object_type, object_id,
                                             {"omero.group": "-1"})
    else:
        raise NotImplementedError(
            f"OMERO object of type {object_type} is not supported")
    assert target is not None, f"{object_type} with ID" \
                               f" {object_id} not found"
    return target


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
    orig_file = _get_original_file(conn, object_type, object_id)

    # Check that the OriginalFile has the expected mimetype
    if unwrap(orig_file.mimetype) != "OMERO.tables":
        raise ValueError(
            f"File {unwrap(orig_file.id)} is not a valid OMERO.tables object")

    # Load the table
    resources = conn.c.sf.sharedResources()
    data_table = resources.openTable(orig_file, {"omero.group": "-1"})
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
                     allow_token=True, interactive=True, keep_alive=True):
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
    :param keep_alive: Periodically ping the server to prevent session timeout.
    :return: OMEROConnection object wrapping a client and Blitz Gateway object,
    with automatic session management and cleanup.
    """
    connector = get_connection(server=server, port=port,
                               session_key=session_key, username=username,
                               password=password, client=client,
                               allow_token=allow_token)
    connector.connect(interactive=interactive, keep_alive=keep_alive)
    return connector
