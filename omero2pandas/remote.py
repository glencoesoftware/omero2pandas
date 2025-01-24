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

import tiledb

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
    LOGGER.info("Writing data to TileDB")
    # Export table
    if isinstance(source, (str, Path)):
        tiledb.from_csv(write_path, source, chunksize=chunk_size)
    else:
        tiledb.from_pandas(write_path, source, chunksize=chunk_size)
    LOGGER.debug("Appending metadata to TileDB")
    # Append omero metadata
    with tiledb.open(write_path, mode="w") as array:
        array.meta['__version'] = OMERO_TILEDB_VERSION
        array.meta['__initialized'] = time.time()
    LOGGER.info("Table saved successfully")
    return write_path
