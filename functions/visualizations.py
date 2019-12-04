import re
from io import BytesIO, StringIO
from typing import Optional, Union

import pandas as pd
from clustergrammer import Network as CGNetwork
from openpyxl import load_workbook
from google.cloud import storage
from cidc_api.models import DownloadableFiles, prism

from .settings import GOOGLE_DATA_BUCKET
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session


def vis_preprocessing(event: dict, context: BackgroundContext):
    with sqlalchemy_session() as session:
        file_id = extract_pubsub_data(event)
        file_record: DownloadableFiles = DownloadableFiles.find_by_id(
            file_id, session=session
        )

        if not file_record:
            raise Exception(f"No downloadable file with id {file_id} found.")

        file_blob = _get_data_file(file_record.object_url)
        metadata_df = _get_metadata_df(file_record.trial_id)

        # Apply the transformations and get derivative data for visualization.
        for transform_name, transform in _get_transforms().items():
            vis_json = transform(file_blob, file_record.data_format, metadata_df)
            if vis_json:
                # Add the vis config to the file_record
                setattr(file_record, transform_name, vis_json)

        # Save the derivative data additions to the database.
        session.commit()


def _get_data_file(
    object_name: str, as_string: bool = False
) -> Union[BytesIO, StringIO]:
    """Download data from GCS to a byte stream and return it."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GOOGLE_DATA_BUCKET)
    blob = bucket.get_blob(object_name)
    file_str = blob.download_as_string()
    if as_string:
        return StringIO(file_str)
    return BytesIO(bytes(file_str))


def _get_metadata_df(trial_id: str) -> pd.DataFrame:
    """
    Build a dataframe containing the participant/sample metadata for this trial,
    joined on CIMAC ID and indexed on CIMAC ID.
    """
    participants_blob = _get_data_file(f"{trial_id}/participants.csv", as_string=True)
    samples_blob = _get_data_file(f"{trial_id}/samples.csv", as_string=True)

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
    that file's `data_format` as arguments, returning
    a JSON blob that the frontend will use for visualization.
    """
    return {"clustergrammer": _ClustergrammerTransform()}


class _ClustergrammerTransform:
    def __call__(
        self, data_file: BytesIO, data_format: str, metadata_df: pd.DataFrame
    ) -> Optional[dict]:
        """
        Prepare the data file for visualization in clustergrammer. 
        NOTE: `metadata_df` should contain data from the participants and samples CSVs
        for this file's trial, joined on CIMAC ID and indexed on CIMAC ID.
        """
        fmt = data_format.lower()
        if not hasattr(self, fmt):
            return None
        return getattr(self, fmt)(data_file, metadata_df)

    def npx(self, data_file, metadata_df: pd.DataFrame) -> dict:
        """Prepare an NPX file for visualization in clustergrammer"""
        # Load the NPX data into a dataframe.
        npx_df = _npx_to_dataframe(data_file)

        # Add category information to `npx_df`'s column headers in the format
        # that Clustergrammer expects:
        #   "([Category 1]: [Value 1], [Category 2]: [Value 2], ...)"
        npx_df_columns_with_categories = metadata_df.loc[npx_df.columns].apply(
            lambda row: (
                f"CIMAC Sample ID: {row.name}",
                f"Participant ID: {row.cimac_participant_id}",
                f"Cohort: {row.cohort_name}",
                f"Collection Event: {row.collection_event_name}",
            ),
            axis=1,
        )
        npx_df.columns = npx_df_columns_with_categories

        # TODO: find a better way to handle missing values
        npx_df.fillna(0, inplace=True)

        # Produce a clustergrammer JSON blob for this dataframe.
        net = CGNetwork()
        net.load_df(npx_df)
        net.normalize()
        net.cluster()
        return net.viz

    def _npx_add_categories(
        self,
        npx_df: pd.DataFrame,
        participants_df: pd.DataFrame,
        samples_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Add participant- and sample-level category metadata to an NPX dataframe 
        in the format that Clustergrammer expects.
        """
        # Join the participants and samples dataframes on CIMAC ID
        clinical_df = pd.merge(
            participants_df,
            samples_df,
            left_on="cimac_participant_id",
            right_on="participants.cimac_participant_id",
            how="outer",
        )
        clinical_df.set_index("cimac_id", inplace=True)

    # TODO: other file types


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
