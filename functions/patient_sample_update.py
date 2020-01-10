from io import StringIO

from google.cloud import storage
from cidc_api.models import DownloadableFiles, TrialMetadata, ManifestUploads, unprism
from collections import namedtuple
from datetime import datetime

from .settings import GOOGLE_DATA_BUCKET
from .util import (
    BackgroundContext,
    extract_pubsub_data,
    sqlalchemy_session,
    make_pseudo_blob,
    get_blob_as_stream as fetch_artifact,
    upload_to_data_bucket,
)


def generate_csvs(event: dict, context: BackgroundContext):
    """
    Given an event containing a manifest upload record ID, generate the 
    patient / sample CSVs for the associated trial, save them to GCS, and create
    DownloadableFiles records representing them.

    NOTE: the name and docstring of this function is potentially out-of-date. 
    The `unprism.derive_files` function may do more than just generate these CSVs.
    """
    upload_id = extract_pubsub_data(event)

    with sqlalchemy_session() as session:
        upload_record: ManifestUploads = ManifestUploads.find_by_id(
            upload_id, session=session
        )
        if not upload_record:
            raise Exception(f"No manifest upload record found with id {upload_id}.")

        # Get trial metadata JSON for the associated trial
        trial_record: TrialMetadata = TrialMetadata.find_by_id(
            upload_record.trial_id, session=session
        )

        # Run the file derivation
        derivation_context = unprism.DeriveFilesContext(
            trial_record.metadata_json, upload_record.manifest_type, fetch_artifact
        )
        derivation_result = unprism.derive_files(derivation_context)

        for artifact in derivation_result.artifacts:
            # Save to GCS
            blob = upload_to_data_bucket(artifact.object_url, artifact.data)

            # Save to database
            DownloadableFiles.create_from_blob(
                trial_record.trial_id,
                artifact.file_type,
                artifact.data_format,
                blob,
                session=session,
            )

        # Update the trial metadata blob (in case the file derivation modified it)
        trial_record.metadata_json = derivation_result.metadata

        session.commit()
