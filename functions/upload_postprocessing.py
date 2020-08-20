from google.cloud import storage
from cidc_api.models import (
    DownloadableFiles,
    TrialMetadata,
    UploadJobs,
    UploadJobStatus,
    unprism,
)
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


def derive_files_from_manifest_upload(event: dict, context: BackgroundContext):
    """
    Generate derivative files from a manifest upload.
    """
    upload_id = extract_pubsub_data(event)

    with sqlalchemy_session() as session:
        upload_record: UploadJobs = UploadJobs.find_by_id(upload_id, session=session)
        if not upload_record:
            raise Exception(f"No manifest upload record found with id {upload_id}.")

        print(f"Received completed manifest upload {upload_id} for postprocessing.")

        # Run the file derivation
        _derive_files_from_upload(
            upload_record.trial_id, upload_record.upload_type, session
        )


def derive_files_from_assay_or_analysis_upload(event: dict, context: BackgroundContext):
    """
    Generate derivative files from an assay or analysis upload.
    """
    upload_id = extract_pubsub_data(event)

    with sqlalchemy_session() as session:
        upload_record: UploadJobs = UploadJobs.find_by_id(upload_id, session=session)

        if not upload_record:
            raise Exception(f"No upload record with id {upload_id} found.")

        if UploadJobStatus(upload_record.status) != UploadJobStatus.MERGE_COMPLETED:
            raise Exception(
                f"Cannot perform postprocessing on upload {upload_id}: status is {upload_record.status}"
            )

        print(
            f"Received completed assay/analysis upload {upload_id} for postprocessing."
        )

        # Run the file derivation
        _derive_files_from_upload(
            upload_record.trial_id, upload_record.upload_type, session
        )


def _derive_files_from_upload(trial_id: str, upload_type: str, session):
    # Get trial metadata JSON for the associated trial
    trial_record: TrialMetadata = TrialMetadata.find_by_trial_id(
        trial_id, session=session
    )

    # Run the file derivation
    derivation_context = unprism.DeriveFilesContext(
        trial_record.metadata_json, upload_type, fetch_artifact
    )
    derivation_result = unprism.derive_files(derivation_context)

    # TODO: consider parallelizing this step if necessary
    for artifact in derivation_result.artifacts:
        # Save to GCS
        blob = upload_to_data_bucket(artifact.object_url, artifact.data)

        # Build basic facet group
        facet_group = f"{artifact.data_format}|{artifact.file_type}"

        # Save to database
        df_record = DownloadableFiles.create_from_blob(
            trial_record.trial_id,
            artifact.file_type,
            artifact.data_format,
            facet_group,
            blob,
            session=session,
            alert_artifact_upload=True,
        )
        df_record.additional_metadata = artifact.metadata
        # Assume that a derived file will be directly useful for data analysis
        df_record.analysis_friendly = True

    # Update the trial metadata blob (in case the file derivation modified it)
    trial_record.metadata_json = derivation_result.trial_metadata

    session.commit()
