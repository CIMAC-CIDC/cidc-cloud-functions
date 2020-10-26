import re
import json
from io import BytesIO
from typing import Optional, Union

import pandas as pd
from clustergrammer import Network as CGNetwork
from openpyxl import load_workbook
from google.cloud import storage
from cidc_api.models import DownloadableFiles, prism

from .settings import GOOGLE_DATA_BUCKET
from .util import (
    BackgroundContext,
    extract_pubsub_data,
    sqlalchemy_session,
    get_blob_as_stream,
)

CLUSTERGRAMMER_MAX_CATEGORY_CARDINALITY = 10


def vis_preprocessing(event: dict, context: BackgroundContext):
    with sqlalchemy_session() as session:
        object_url = extract_pubsub_data(event)
        file_record: DownloadableFiles = DownloadableFiles.get_by_object_url(
            object_url, session=session
        )

        if not file_record:
            raise Exception(f"No downloadable file with object URL {object_url} found.")

        # Apply the transformations and get derivative data for visualization.
        for transform_name, transform in _get_transforms().items():
            vis_json = transform(file_record)
            if vis_json: # Add the vis config to the file_record
                setattr(file_record, transform_name, vis_json)

        # Save the derivative data additions to the database.
        session.commit()


def _get_metadata_df(trial_id: str) -> pd.DataFrame:
    """
    Build a dataframe containing the participant/sample metadata for this trial,
    joined on CIMAC ID and indexed on CIMAC ID.
    """
    participants_blob = get_blob_as_stream(
        f"{trial_id}/participants.csv", as_string=True
    )
    samples_blob = get_blob_as_stream(f"{trial_id}/samples.csv", as_string=True)

    participants_df = pd.read_csv(participants_blob)
    samples_df = pd.read_csv(samples_blob)

    metadata_df = pd.merge(
        participants_df,
        samples_df,
        left_on="cimac_participant_id",
        right_on="participants.cimac_participant_id",
        how="outer",
    )
    metadata_df.set_index("cimac_id", inplace=True)

    return metadata_df


def _get_transforms() -> dict:
    """ 
    Get a list of functions taking an open file and
    that file's downloadable file record as arguments, returning
    a JSON blob that the frontend will use for visualization.
    """
    return {
        "clustergrammer": _clustergrammer_transform,
        "ihc_combined_plot": _ihc_combined_transform,
    }


def _ihc_combined_transform(file_record: DownloadableFiles) -> Optional[dict]:
    """
    Prepare an IHC combined file for visualization by joining it with relevant metadata
    """
    if file_record.upload_type.lower() != "ihc marker combined":
        return None

    assert file_record.data_format.lower() == "csv"

    print(f"Generating IHC combined visualization config for file {file_record.id}")
    data_file = get_blob_as_stream(file_record.object_url)
    metadata_df = _get_metadata_df(file_record.trial_id)

    data_df = pd.read_csv(data_file)
    full_df = data_df.join(metadata_df, on="cimac_id", how="inner")

    return json.loads(full_df.to_json(orient="records"))


def _clustergrammer_transform(file_record: DownloadableFiles) -> Optional[dict]:
    """
    Prepare the data file for visualization in clustergrammer. 
    NOTE: `metadata_df` should contain data from the participants and samples CSVs
    for this file's trial, joined on CIMAC ID and indexed on CIMAC ID.
    """
    if file_record.data_format.lower() not in (
        "npx",
        "cell counts compartment",
        "cell counts assignment",
        "cell counts profiling",
    ):
        return None

    file_blob = get_blob_as_stream(file_record.object_url)
    metadata_df = _get_metadata_df(file_record.trial_id)

    if file_record.data_format.lower() == "npx":
        data_df = _npx_to_dataframe(file_blob)
    else:
        data_df = _cytof_summary_to_dataframe(file_blob)

    assert (
        data_df.shape[1] > 1
    ), "Cannot generate clustergrammer visualization for data with only one sample."

    data_df_columns_with_categories = _metadata_to_categories(
        metadata_df.loc[data_df.columns]
    )
    data_df.columns = data_df_columns_with_categories

    # TODO: find a better way to handle missing values
    data_df.fillna(0, inplace=True)

    # Produce a clustergrammer JSON blob for this dataframe.
    net = CGNetwork()
    net.load_df(data_df)
    net.normalize()
    net.cluster()
    return net.viz


def _metadata_to_categories(metadata_df: pd.DataFrame) -> list:
    """
    Add category information to `data_df`'s column headers in the format that Clustergrammer expects:
        "([Category 1]: [Value 1], [Category 2]: [Value 2], ...)"
    """
    metadata_df = metadata_df.dropna(
        axis=1
    )  # make sure we only use categories with full data
    for c in metadata_df.columns:
        cardinality = len(metadata_df[c].unique())
        if cardinality > CLUSTERGRAMMER_MAX_CATEGORY_CARDINALITY or cardinality <= 1:
            if c not in [
                "cimac_participant_id",
                "cohort_name",
                "collection_event_name",
            ]:
                metadata_df.pop(c)

    ret = []
    for idx, row in metadata_df.iterrows():
        temp = [f"CIMAC Sample ID: {idx}"]

        for cat, val in row.items():
            cat = (
                cat.replace("_", " ")
                .title()
                .replace("Cimac", "CIMAC")
                .replace("Cidc", "CIDC")
                .replace("Id", "ID")
                .rsplit("Name", 1)[0]
                .strip()
            )
            temp.append(f"{cat}: {val}")

        ret.append(tuple(temp))

    return ret


def _npx_to_dataframe(fname, sheet_name="NPX Data") -> pd.DataFrame:
    """Load raw data from an NPX file into a pandas dataframe."""

    wb = load_workbook(fname)
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Couldn't locate expected worksheet '{sheet_name}'.")
    ws = wb[sheet_name]

    extract_values = lambda xlsx_row: [cell.value for cell in xlsx_row]

    # Assay labels (row 5 of the spreadsheet)
    assay_labels = extract_values(ws[4][1:-2])

    # Raw data (row 8 of the spreadsheet onwards)
    rows = []
    num_cols = len(assay_labels) + 1
    for row in ws.iter_rows(min_row=8):
        sample_id = row[0].value
        # If we hit a blank line, there's no more data to read.
        if not sample_id:
            break
        # Only include rows pertaining to CIMAC ids
        if prism.cimac_id_regex.match(sample_id):
            new_row = extract_values(row[0:num_cols])
            rows.append(new_row)
    raw = pd.DataFrame(rows).set_index(0)
    raw.index.name = "cimac_id"
    raw.columns = assay_labels

    # Drop columns that don't have raw data
    raw.drop(columns=["Plate ID", "QC Warning"], inplace=True)

    return raw.T


def _cytof_summary_to_dataframe(csv: BytesIO) -> pd.DataFrame:
    """Load a CyTOF summary CSV into a dataframe with CIMAC IDs as column headers"""
    raw_df = pd.read_csv(csv)

    # Index on CIMAC ID column
    indexed_df = raw_df.set_index("cimac_id")

    # Drop unused metadata columns (we should get these from the metadata df)
    for col in ["cimac_participant_id", "protocol_identifier"]:
        try:
            indexed_df.drop(col, axis=1, inplace=True)
        except KeyError:
            pass

    return indexed_df.T
