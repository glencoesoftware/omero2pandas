# encoding: utf-8
#
# Copyright (c) 2025 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import os
import io
import logging

import omero
from omero.rtypes import unwrap
from omero.gateway import BlitzGateway

LOGGER = logging.getLogger(__name__)


class OriginalFileIO(io.RawIOBase):
    """
    Reader for loading data from OMERO OriginalFile objects with a
    Python file-like interface

    For optimal performance this class should be wrapped in a BufferedReader,
    use the get_annotation_reader convenience function for this.
    """
    def __init__(self, conn: BlitzGateway, file_id: int, reporter=None):
        LOGGER.debug(f"Creating new  for file {file_id}")
        self._prx = conn.c.getSession().createRawFileStore()
        self._file_id = file_id
        self.open()
        if not (size := self._prx.size()):
            raise omero.ClientError(f"Invalid size for OriginalFile {file_id}")
        self._size = size
        self._offset = 0
        self._reporter = reporter
        if self._reporter is not None:
            reporter.reset(total=size)

    def __enter__(self):
        self.open()

    def __exit__(self, **kwargs):
        self.close()
        if self._reporter is not None:
            self._reporter.close()

    def open(self):
        current = unwrap(self._prx.getFileId())
        if current is None:
            # RawFileStore is new or was previously closed
            LOGGER.debug(f"Setting IO file ID to {self._file_id}")
            self._prx.setFileId(self._file_id, {"omero.group": "-1"})
        elif current != self._file_id:
            # Something else is messing with our filestore
            raise Exception("RawFileStore pointed to incorrect object")

    def read(self, size=-1):
        # Read `size` bytes from the target object. -1 = 'read all'
        LOGGER.debug(f"Reading {size} bytes from file {self._file_id}")
        # Ensure we're reading the correct object
        if size == -1:
            size = self._size - self._offset
        size = min(size, self._size - self._offset)
        if not size or self._offset >= self._size:
            LOGGER.debug("Nothing to read")
            return b""
        data = self._prx.read(self._offset, size)
        self._offset += size
        if self._reporter is not None:
            self._reporter.update(size)
        return data

    def readinto(self, buffer):
        # Read bytes into the provided object and return number read
        if self._offset >= self._size:
            return 0
        data = self.read(len(buffer))
        count = len(data)
        buffer[:count] = data
        return count

    def seek(self, offset, whence=os.SEEK_SET):
        # Seek within the target file
        if whence == os.SEEK_SET:
            self._offset = offset
        elif whence == os.SEEK_CUR:
            self._offset += offset
        elif whence == os.SEEK_END:
            self._offset = self._size - offset
        else:
            raise ValueError(f"Invalid whence value: {whence}")
        if self._offset > self._size:
            self._offset = self._size
        elif self._offset < 0:
            self._offset = 0

    def close(self):
        file_id = unwrap(self._prx.getFileId())
        if file_id == self._file_id:
            # We only close if the current file is the active one
            LOGGER.debug("Closing reader")
            self._prx.close()

    def size(self):
        return self._size

    def tell(self):
        return self._offset

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False


def get_annotation_reader(conn, file_id, chunk_size, reporter=None):
    """
    Fetch a buffered reader for loading OriginalFile data
    :param conn: BlitzGateway connection object
    :param file_id: OriginalFile ID to load
    :param chunk_size: Number of bytes to load per server call
    :param reporter: TQDM progressbar instance
    :return: BufferedReader instance
    """
    reader = OriginalFileIO(conn, file_id, reporter=reporter)
    return io.BufferedReader(reader, buffer_size=chunk_size)


def infer_compression(mimetype, name):
    # Validate that the suggested file is actually some sort of CSV
    if mimetype is None:
        if name.lower().endswith(".csv"):
            mimetype = "text/csv"
        elif name.lower().endswith(".csv.gz"):
            mimetype = "application/x-gzip"
        else:
            raise ValueError(f"Unsupported filetype: {name}")
    mimetype = mimetype.lower()
    if mimetype == "application/x-gzip":
        return "gzip"
    elif mimetype == "text/csv":
        return None
    raise ValueError(f"Unsupported mimetype: {mimetype}")
