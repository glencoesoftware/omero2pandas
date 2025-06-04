# encoding: utf-8
#
# Copyright (c) Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import logging
import math
import os
from pathlib import Path

import omero
import omero.grid
import pandas as pd
from tqdm.auto import tqdm

LOGGER = logging.getLogger(__name__)

SPECIAL_NAMES = {
    'roi': omero.grid.RoiColumn,
    'image': omero.grid.ImageColumn,
    'dataset': omero.grid.DatasetColumn,
    'well': omero.grid.WellColumn,
    'field': omero.grid.ImageColumn,
    'wellsample': omero.grid.ImageColumn,
    'plate': omero.grid.PlateColumn,
}

COLUMN_TYPES = {
    'i': omero.grid.LongColumn,
    'u': omero.grid.LongColumn,
    'f': omero.grid.DoubleColumn,
    'O': omero.grid.StringColumn,
    'S': omero.grid.StringColumn,
    'U': omero.grid.StringColumn,
    'b': omero.grid.BoolColumn,
}

LINK_TYPES = {
    "Image": omero.model.ImageAnnotationLinkI,
    "Dataset": omero.model.DatasetAnnotationLinkI,
    "Plate": omero.model.PlateAnnotationLinkI,
    "Project": omero.model.ProjectAnnotationLinkI,
    "Screen": omero.model.ScreenAnnotationLinkI,
    "Well": omero.model.WellAnnotationLinkI,
    "Roi": omero.model.RoiAnnotationLinkI,
}

OBJECT_TYPES = {
    "Image": omero.model.ImageI,
    "Dataset": omero.model.DatasetI,
    "Plate": omero.model.PlateI,
    "Project": omero.model.ProjectI,
    "Screen": omero.model.ScreenI,
    "Well": omero.model.WellI,
    "Roi": omero.model.RoiI,
    "BooleanAnnotation": omero.model.BooleanAnnotationI,
    "CommentAnnotation": omero.model.CommentAnnotationI,
    "DoubleAnnotation": omero.model.DoubleAnnotationI,
    "FileAnnotation": omero.model.FileAnnotationI,
    "ListAnnotation": omero.model.ListAnnotationI,
    "LongAnnotation": omero.model.LongAnnotationI,
    "MapAnnotation": omero.model.MapAnnotationI,
    "TagAnnotation": omero.model.TagAnnotationI,
    "TermAnnotation": omero.model.TermAnnotationI,
    "TimestampAnnotation": omero.model.TimestampAnnotationI,
    "XmlAnnotation": omero.model.XmlAnnotationI,
}


def optimal_chunk_size(column_count):
    # We can optimally send ~2m values at a time
    rows = 2000000 // column_count
    if rows > 50000:
        LOGGER.warning(f"Limiting automatic chunk size to 50000 (was {rows})")
    return max(min(rows, 50000), 1)


def generate_omero_columns(df):
    # Inspect a pandas dataframe to generate OMERO.tables columns
    omero_columns = []
    string_columns = []
    for column_name, column_type in df.dtypes.items():
        cleaned_name = column_name.replace('/', '\\')
        if column_name.lower() in SPECIAL_NAMES and column_type.kind == 'i':
            col_class = SPECIAL_NAMES[column_name.lower()]
        elif column_type.kind in COLUMN_TYPES:
            col_class = COLUMN_TYPES[column_type.kind]
        else:
            raise NotImplementedError(f"Column type "
                                      f"{column_type} not supported")
        if col_class == omero.grid.StringColumn:
            string_columns.append(column_name)
            max_len = df[column_name].str.len().max()
            if math.isnan(max_len):
                max_len = 1
            col = col_class(cleaned_name, "", int(max_len), [])
        else:
            col = col_class(cleaned_name, "", [])
        omero_columns.append(col)
    return omero_columns, string_columns


def generate_omero_columns_csv(csv_path, chunk_size=1000):
    # Inspect a CSV file to generate OMERO.tables columns
    LOGGER.info(f"Inspecting {csv_path}")
    scan = pd.read_csv(csv_path, nrows=chunk_size or 1000)
    LOGGER.debug(f"Shape is {scan.shape[0]}x{scan.shape[1]}")
    if chunk_size is None:
        chunk_size = optimal_chunk_size(len(scan.columns))
    LOGGER.debug(f"Using chunk size {chunk_size}")
    omero_columns = []
    to_resolve = {}
    for idx, (column_name, column_type) in enumerate(scan.dtypes.items()):
        cleaned_name = column_name.replace('/', '\\')
        if column_name in SPECIAL_NAMES and column_type.kind == 'i':
            col_class = SPECIAL_NAMES[column_name]
        elif column_type.kind in COLUMN_TYPES:
            col_class = COLUMN_TYPES[column_type.kind]
        else:
            raise NotImplementedError(f"Column type "
                                      f"{column_type} not supported")
        if col_class == omero.grid.StringColumn:
            max_len = scan[column_name].str.len().max()
            if math.isnan(max_len):
                max_len = 1
            col = col_class(cleaned_name, "", int(max_len), [])
            to_resolve[column_name] = idx
        else:
            col = col_class(cleaned_name, "", [])
        omero_columns.append(col)
    LOGGER.debug(f"Generated columns, found {len(to_resolve)} string columns")
    # Use a subset of columns to get row count and string lengths
    use_cols = to_resolve.keys() or [0]
    row_count = 0
    LOGGER.info("Scanning CSV for size and column metadata")
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, usecols=use_cols):
        # chunk is a DataFrame. To "process" the rows in the chunk:
        row_count += len(chunk)
        for column_name, index in to_resolve.items():
            max_len = chunk[column_name].str.len().max()
            if math.isnan(max_len):
                max_len = 1
            max_len = int(max_len)
            omero_columns[index].size = max(max_len, omero_columns[index].size)
    LOGGER.info(f"Initial scan completed, found {row_count} rows")
    return omero_columns, to_resolve.keys(), row_count, chunk_size


def create_table(source, table_name, links, conn, chunk_size):
    # Create an OMERO.table and upload data
    # Make type case-insensitive, handling annotation caps properly
    links = [(t.lower().capitalize().replace("annotation", "Annotation"), i)
             for t, i in links]
    # Validate link list
    working_group = None
    roi_only = True
    for target_type, target_id in links:
        if target_type not in OBJECT_TYPES:
            raise NotImplementedError(f"Type {target_type} not "
                                      f"supported as a link target")
        if target_type != "Roi":
            roi_only = False
        target_ob = conn.getQueryService().find(target_type, target_id,
                                                {"omero.group": "-1"})
        if target_ob is None:
            raise ValueError(f"{target_type} #{target_id} not found")
        target_group = target_ob.details.group.id.val
        if working_group is None:
            working_group = target_group
        else:
            if working_group != target_group:
                raise ValueError("All objects being linked to must belong to "
                                 "the same OMERO group")
    if roi_only:
        LOGGER.warning("Only ROIs have been selected to link the table to. "
                       "Resulting table may not be shown in the OMERO.web UI.")
    conn.SERVICE_OPTS.setOmeroGroup(working_group)

    progress_monitor = tqdm(
        desc="Inspecting table...", initial=1, dynamic_ncols=True,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| '
                   '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}')

    if isinstance(source, (str, Path)):
        assert os.path.exists(source), f"Could not find file {source}"
        columns, str_cols, total_rows, chunk_size = generate_omero_columns_csv(
            source, chunk_size)
        iter_data = (chunk for chunk in pd.read_csv(
            source, chunksize=chunk_size))
    else:
        source = source.copy()
        columns, str_cols = generate_omero_columns(source)
        total_rows = len(source)
        if chunk_size is None:
            chunk_size = optimal_chunk_size(len(columns))
        LOGGER.debug(f"Using chunk size {chunk_size}")
        iter_data = (source.iloc[i:i + chunk_size]
                     for i in range(0, len(source), chunk_size))

    resources = conn.c.sf.sharedResources({"omero.group": str(working_group)})
    repository_id = resources.repositories().descriptions[0].getId().getValue()

    table = None
    try:
        table = resources.newTable(repository_id, table_name,
                                   {"omero.group": str(working_group)})
        table.initialize(columns)
        progress_monitor.reset(total=total_rows)
        progress_monitor.set_description("Uploading table to OMERO")

        for chunk in iter_data:
            if str_cols:
                # Coerce missing values into strings
                chunk.loc[:, str_cols] = chunk.loc[:, str_cols].fillna('')
            for omero_column, (name, col_data) in zip(columns, chunk.items()):
                if omero_column.name != name:
                    LOGGER.debug(f"Matching {omero_column.name} -> {name}")
                omero_column.values = col_data.tolist()
            table.addData(columns)
            progress_monitor.update(len(chunk))
        progress_monitor.close()

        LOGGER.info("Table creation complete, linking to image")
        orig_file = table.getOriginalFile()
        # Create FileAnnotation from OriginalFile
        annotation = omero.model.FileAnnotationI()
        annotation.file = omero.model.OriginalFileI(orig_file.id.val, False)
        annotation_obj = conn.getUpdateService().saveAndReturnObject(
            annotation, {"omero.group": str(working_group)})
        LOGGER.info(f"Generated FileAnnotation {annotation_obj.id.val} "
                    f"with OriginalFile {orig_file.id.val}, "
                    f"linking to targets")
        # Link the FileAnnotation to all targets
        unloaded_annotation = omero.model.FileAnnotationI(
            annotation_obj.id.val, False)
        link_buffer = []
        for obj_type, obj_id in links:
            if "annotation" in obj_type.lower():
                link_obj = omero.model.AnnotationAnnotationLinkI()
            else:
                link_obj = LINK_TYPES[obj_type]()
            unloaded_target = OBJECT_TYPES[obj_type](obj_id, False)
            link_obj.link(unloaded_target, unloaded_annotation)
            link_buffer.append(link_obj)
        # Transmit links to server
        conn.getUpdateService().saveArray(
            link_buffer, {"omero.group": str(working_group)})
        LOGGER.info(f"Finished creating table {table_name}")
        return annotation_obj.id.val
    finally:
        if table is not None:
            table.close()
