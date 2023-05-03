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

import omero
import omero.grid
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
    "Well": omero.model.WellAnnotationLinkI,
}

OBJECT_TYPES = {
    "Image": omero.model.ImageI,
    "Dataset": omero.model.DatasetI,
    "Plate": omero.model.PlateI,
    "Well": omero.model.WellI,
}


def generate_omero_columns(df):
    omero_columns = []
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
            max_len = df[column_name].str.len().max()
            if math.isnan(max_len):
                max_len = 1
            col = col_class(cleaned_name, "", max_len, [])
        else:
            col = col_class(cleaned_name, "", [])
        omero_columns.append(col)
    return omero_columns


def create_table(df, table_name, parent_id, parent_type, conn, chunk_size):
    if parent_type not in OBJECT_TYPES:
        raise NotImplementedError(f"Type {parent_type} not "
                                  f"supported as a parent object")
    parent_ob = conn.getObject(parent_type, parent_id)
    if parent_ob is None:
        raise ValueError(f"{parent_type} ID {parent_id} not found")
    parent_group = parent_ob.details.group.id.val

    orig_columns = df.columns.tolist()
    columns = generate_omero_columns(df)
    resources = conn.c.sf.sharedResources(_ctx={
        "omero.group": str(parent_group)})
    repository_id = resources.repositories().descriptions[0].getId().getValue()

    table = None
    try:
        table = resources.newTable(repository_id, table_name, _ctx={
            "omero.group": str(parent_group)})
        table.initialize(columns)
        total_to_upload = len(df)
        slicer = range(0, total_to_upload, chunk_size)

        bar_fmt = '{desc}: {percentage:3.0f}%|{bar}| ' \
                  '{n_fmt}/{total_fmt} rows, {elapsed} {postfix}'

        chunk_iter = tqdm(desc="Uploading table to OMERO",
                          total=total_to_upload,
                          bar_format=bar_fmt)
        for start in slicer:
            to_upload = df[start:start + chunk_size]
            for idx, column in enumerate(columns):
                ref_name = orig_columns[idx]
                column.values = to_upload[ref_name].tolist()
            table.addData(columns)
            chunk_iter.update(len(to_upload))
        chunk_iter.close()

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
        conn.getUpdateService().saveObject(link_obj, _ctx={
            "omero.group": str(parent_group)})
        LOGGER.info("Saved annotation link")

        LOGGER.info(f"Finished creating table {table_name} under "
                    f"{parent_type} {parent_id}")
        return orig_file.id.val
    finally:
        if table is not None:
            table.close()
