# encoding: utf-8
#
# Copyright (c) Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import logging
import secrets
from pathlib import Path, PurePosixPath
import time

import pandas as pd
import requests
import tiledb
from requests import HTTPError
from tqdm.auto import tqdm

from omero2pandas.connect import get_connection

LOGGER = logging.getLogger(__name__)

OMERO_TILEDB_VERSION = '3'  # Version of the omero table implementation
CSRF_TOKEN_HEADER = "X-CSRFToken"
SEC_TOKEN_HEADER = "X-SecretToken"
SEC_TOKEN_METADATA_KEY = 'SecretToken'  # Metadata key for secret token
TOKEN_ENDPOINT = "/api/v0/token"
REGISTER_ENDPOINT = "/omero_plus/api/v0/table"


def create_remote_table(source, table_name, local_path, remote_path=None,
                        links=(), chunk_size=1000, connector=None,
                        prefix=""):
    LOGGER.info("Registering remote table")
    # Default filters from tiledb.from_pandas()
    write_path = Path(local_path)
    if write_path.is_dir() and not write_path.name.endswith(".tiledb"):
        # Generate file name from the table name if not provided
        write_path = (write_path / table_name).with_suffix(".tiledb")
    # Assume the server will be running on Linux
    if remote_path is None:
        remote_path = PurePosixPath(write_path)
    else:
        remote_path = PurePosixPath(remote_path)
        if remote_path.suffix != '.tiledb':
            remote_path = remote_path / write_path.name
    LOGGER.debug(f"Remote path would be {str(remote_path)}")
    token = create_tiledb(source, write_path, chunk_size=chunk_size)
    ann_id = register_table(connector, remote_path, table_name, links, token,
                            prefix=prefix)
    return ann_id


def create_tiledb(source, output_path, chunk_size=1000):
    if not isinstance(output_path, Path):
        # Convert strings to proper path objects
        output_path = Path(output_path)
    if output_path.exists():
        raise ValueError(f"Table file {output_path} already exists")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # path.as_uri() exists but mangles any spaces in the path!
    output_path = str(output_path)
    # Use a default chunk size if not set
    chunk_size = chunk_size or 1000
    LOGGER.info("Writing data to TileDB")
    # Export table
    if isinstance(source, (str, Path)):
        data_iterator = pd.read_csv(source, chunksize=chunk_size)
        total_rows = None
    else:
        data_iterator = (source.iloc[i:i + chunk_size]
                         for i in range(0, len(source), chunk_size))
        total_rows = len(source)
    progress_monitor = tqdm(
        desc="Generating TileDB file...", initial=1, dynamic_ncols=True,
        total=total_rows,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| '
                   '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}')
    row_idx = 0
    for chunk in data_iterator:
        tiledb.from_pandas(output_path, chunk, sparse=True, full_domain=True,
                           tile=10000, attr_filters=None,
                           row_start_idx=row_idx, allows_duplicates=False,
                           mode="append" if row_idx else "ingest")
        progress_monitor.update(len(chunk))
        row_idx += len(chunk)
    progress_monitor.close()
    LOGGER.debug("Appending metadata to TileDB")
    # Append omero metadata
    security_token = secrets.token_urlsafe()
    with tiledb.open(output_path, mode="w") as array:
        array.meta['__version'] = OMERO_TILEDB_VERSION
        array.meta['__initialized'] = time.time()
        array.meta[SEC_TOKEN_METADATA_KEY] = security_token
    LOGGER.info("Table saved successfully")
    return security_token


def register_table(connector, remote_path, table_name, links, token,
                   prefix=""):
    # Check we got the correct connector type
    connector = get_connection(client=connector)
    if not connector.server:
        raise ValueError("Unknown server? This should never happen!")
    server = f"https://{connector.server}"
    # Fix malformed prefix arguments if provided
    if prefix and not prefix.startswith("/"):
        prefix = f"/{prefix}"
    if prefix.endswith("/"):
        prefix = prefix[:-1]
    # Determine endpoint URLs to use. Must be HTTPS
    token_url = f"{server}{prefix}{TOKEN_ENDPOINT}"
    target_url = (f"{server}{prefix}{REGISTER_ENDPOINT}"
                  f"?bsession={connector.getSessionId()}")
    # We first need to get a CSRF security token and cookie from the server
    LOGGER.debug(f"Fetching CSRF token from {connector.server}")
    token_result = requests.get(token_url)
    token_data = _check_response(token_result)
    # Now that we have the token, construct the POST to do registration
    payload = {
        "uri": str(remote_path),
        "name": table_name,
        "targets": [f"{kind}:{ob_id}" for kind, ob_id in links],
    }
    headers = {
        "Content-Type": "application/json",
        SEC_TOKEN_HEADER: token,
        CSRF_TOKEN_HEADER: token_data["data"],
        "Referer": server,
    }
    LOGGER.info(f"Registering table to {connector.server}")
    LOGGER.debug(f"Request params: {payload=}, {headers=}, url={target_url}")
    result = requests.post(url=target_url, json=payload, headers=headers,
                           cookies=token_result.cookies, allow_redirects=False)
    content = _check_response(result)
    ann_id = content["data"]["file_annotation"]
    LOGGER.info(f"Registered table successfully as FileAnnotation {ann_id}")
    return ann_id


def _check_response(response):
    # Check response from an OMERO HTTP request and show error messages
    if 200 <= response.status_code < 300:
        return response.json()
    error_message = "<No further message>"
    if response.headers.get("content-type") == "application/json":
        error_message = response.json()
        if "message" in error_message:
            error_message = error_message["message"]
    LOGGER.error(
        f"Request returned HTTP code {response.status_code}: {error_message}")
    response.raise_for_status()
    raise HTTPError(f"Unhandled response code: {response.status_code}")
