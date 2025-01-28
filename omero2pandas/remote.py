# encoding: utf-8
#
# Copyright (c) Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import logging
from pathlib import Path, PurePosixPath
import time

import pandas as pd
import tiledb
from tqdm.auto import tqdm

LOGGER = logging.getLogger(__name__)

OMERO_TILEDB_VERSION = '3'  # Version of the omero table implementation


def register_table(source, chunk_size, local_path, remote_path):
    LOGGER.info("Registering remote table")
    # Default filters from tiledb.from_pandas()
    write_path = Path(local_path or remote_path).with_suffix(".tiledb")
    # Assume the server will be running on Linux
    remote_path = PurePosixPath(
        remote_path or local_path).with_suffix(".tiledb")
    LOGGER.debug(f"Remote path would be {str(remote_path)}")
    if write_path.exists():
        raise ValueError(f"Table file {write_path} already exists")
    # path.as_uri() exists but mangles any spaces in the path!
    write_path = str(write_path)
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
        tiledb.from_pandas(write_path, chunk, sparse=True, full_domain=True,
                           tile=10000, attr_filters=None,
                           row_start_idx=row_idx, allows_duplicates=False,
                           mode="append" if row_idx else "ingest")
        progress_monitor.update(len(chunk))
        row_idx += len(chunk)
    progress_monitor.close()
    LOGGER.debug("Appending metadata to TileDB")
    # Append omero metadata
    with tiledb.open(write_path, mode="w") as array:
        array.meta['__version'] = OMERO_TILEDB_VERSION
        array.meta['__initialized'] = time.time()
    LOGGER.info("Table saved successfully")
    return write_path
