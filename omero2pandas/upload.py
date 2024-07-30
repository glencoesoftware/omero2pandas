# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.
import logging
import math
import os
from typing import Iterable

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
}


def generate_omero_columns(df):
    omero_columns = []
    string_columns = []
    for column_name, column_type in df.dtypes.items():
        cleaned_name = column_name.replace('/', '\\')
        if column_name in SPECIAL_NAMES and column_type.kind == 'i':
            col_class = SPECIAL_NAMES[column_name]
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


def create_table(source, table_name, parent_id, parent_type, conn, chunk_size,
                 extra_links):
    # Make type case-insensitive
    parent_type = parent_type.lower().capitalize()
    if parent_type not in OBJECT_TYPES:
        raise NotImplementedError(f"Type {parent_type} not "
                                  f"supported as a parent object")
    elif parent_type == "Roi":
        LOGGER.warning("ROI selected as the primary attachment target, "
                       "resulting table may not be shown in OMERO.web UI.")
    parent_ob = conn.getObject(parent_type, parent_id)
    if parent_ob is None:
        raise ValueError(f"{parent_type} ID {parent_id} not found")
    parent_group = parent_ob.details.group.id.val
    if extra_links is not None and not isinstance(extra_links, Iterable):
        raise ValueError(f"Extra Links should be an iterable list of "
                         f"type/id pairs, not {extra_links}")
    link_to = []
    for ob_type, ob_id in extra_links:
        ob_type = ob_type.lower().capitalize()
        if ob_type not in OBJECT_TYPES:
            raise NotImplementedError(f"Type {ob_type} not "
                                      f"supported as a link target")
        if isinstance(ob_id, str):
            assert ob_id.isdigit(), f"Object ID {ob_id} is not numeric"
            ob_id = int(ob_id)
        link_ob = conn.getObject(ob_type, ob_id)
        if link_ob is None:
            LOGGER.warning(f"{ob_type} ID {ob_id} not found, won't link")
            continue
        link_to.append((ob_type, ob_id))

    progress_monitor = tqdm(
        desc="Inspecting table...", initial=1, dynamic_ncols=True,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| '
                   '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}')

    if isinstance(source, str):
        assert os.path.exists(source), f"Could not find file {source}"
        columns, str_cols, total_rows = generate_omero_columns_csv(
            source, chunk_size)
        iter_data = (chunk for chunk in pd.read_csv(
            source, chunksize=chunk_size))
    else:
        source = source.copy()
        columns, str_cols = generate_omero_columns(source)
        total_rows = len(source)
        iter_data = (source.iloc[i:i + chunk_size]
                     for i in range(0, len(source), chunk_size))

    resources = conn.c.sf.sharedResources(_ctx={
        "omero.group": str(parent_group)})
    repository_id = resources.repositories().descriptions[0].getId().getValue()

    table = None
    try:
        table = resources.newTable(repository_id, table_name, _ctx={
            "omero.group": str(parent_group)})
        table.initialize(columns)
        progress_monitor.reset(total=total_rows)
        progress_monitor.set_description("Uploading table to OMERO")

        for chunk in iter_data:
            if str_cols:
                # Coerce missing values into strings
                chunk.loc[:, str_cols] = chunk.loc[:, str_cols].fillna('')
            for omero_column, (name, col_data) in zip(columns, chunk.items()):
                if omero_column.name != name:
                    LOGGER.debug("Matching", omero_column.name, name)
                omero_column.values = col_data.tolist()
            table.addData(columns)
            progress_monitor.update(len(chunk))
        progress_monitor.close()

        LOGGER.info("Table creation complete, linking to image")
        orig_file = table.getOriginalFile()

        # create file link
        link_obj = LINK_TYPES[parent_type]()
        target_obj = OBJECT_TYPES[parent_type](parent_id, False)
        # create annotation
        annotation = omero.model.FileAnnotationI()
        # link table to annotation object
        annotation.file = orig_file

        link_obj.link(target_obj, annotation)
        link_obj = conn.getUpdateService().saveAndReturnObject(
            link_obj, _ctx={"omero.group": str(parent_group)})
        annotation_id = link_obj.child.id.val
        LOGGER.info(f"Uploaded as FileAnnotation {annotation_id}")
        extra_link_objs = []
        unloaded_ann = omero.model.FileAnnotationI(annotation_id, False)
        for ob_type, ob_id in link_to:
            # Construct additional links
            link_obj = LINK_TYPES[ob_type]()
            target_obj = OBJECT_TYPES[ob_type](ob_id, False)
            link_obj.link(target_obj, unloaded_ann)
            extra_link_objs.append(link_obj)
        if extra_link_objs:
            try:
                conn.getUpdateService().saveArray(
                    extra_link_objs, _ctx={"omero.group": str(parent_group)})
                LOGGER.info(f"Added links to {len(extra_link_objs)} objects")
            except Exception as e:
                LOGGER.error("Failed to create extra links", exc_info=e)
        LOGGER.info(f"Finished creating table {table_name} under "
                    f"{parent_type} {parent_id}")
        return annotation_id
    finally:
        if table is not None:
            table.close()


def generate_omero_columns_csv(csv_path, chunk_size=1000):
    LOGGER.info(f"Inspecting {csv_path}")
    scan = pd.read_csv(csv_path, nrows=chunk_size)
    LOGGER.debug("Shape is ", scan.shape)
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
    return omero_columns, to_resolve.keys(), row_count
