from google.cloud import storage
from cidc_api.models import DownloadableFiles, TrialMetadata, ExtraDataTypes
from os import environ
from collections import namedtuple
from datetime import datetime

from .settings import GOOGLE_DATA_BUCKET
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session


def generate_csvs(event: dict, context: BackgroundContext):
    """
    Given an event containing a trial metadata record ID, generate the 
    patient / sample CSVs for that metadata, save them to GCS, and create
    DownloadableFiles records representing them.
    """
    trial_id = extract_pubsub_data(event)

    with sqlalchemy_session() as session:
        # Generate the CSVs
        patient_csv = TrialMetadata.generate_patient_csv(trial_id, session=session)
        patient_name = f"{trial_id}/participants.csv"
        sample_csv = TrialMetadata.generate_sample_csv(trial_id, session=session)
        sample_name = f"{trial_id}/samples.csv"

        # Upload the CSVs to GCS
        patient_blob = _upload_to_data_bucket(patient_name, patient_csv)
        sample_blob = _upload_to_data_bucket(sample_name, sample_csv)

        # Save to DownloadableFiles
        DownloadableFiles.create_from_blob(
            trial_id,
            ExtraDataTypes.PARTICIPANTS_INFO.value,
            "csv",
            patient_blob,
            session=session,
        )
        DownloadableFiles.create_from_blob(
            trial_id,
            ExtraDataTypes.SAMPLES_INFO.value,
            "csv",
            sample_blob,
            session=session,
        )


_pseudo_blob = namedtuple(
    "_pseudo_blob", ["name", "size", "md5_hash", "crc32c", "time_created"]
)


def _upload_to_data_bucket(name: str, csv: str):
    """Upload a CSV to blob called `name` in the CIDC data bucket."""
    if environ.get("FLASK_ENV") == "development":
        fname = name.replace("/", "_")
        print(f"writing {fname}")
        with open(fname, "w") as f:
            f.write(csv)
        return _pseudo_blob(
            f"./{fname}", 0, "_pseudo_md5_hash", "_pseudo_crc32c", datetime.now()
        )

    client = storage.Client()
    bucket = client.get_bucket(GOOGLE_DATA_BUCKET)
    blob = bucket.blob(name)
    blob.upload_from_string(csv)

    return blob
