"""A pub/sub triggered functions that respond to data upload events"""
from flask import jsonify
from google.cloud import storage
from cidc_api.models import AssayUploads, TrialMetadata, DownloadableFiles

from .settings import GOOGLE_DATA_BUCKET, GOOGLE_UPLOAD_BUCKET
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session


def ingest_upload(event: dict, context: BackgroundContext):
    """
    When a successful upload event is published, move the data associated
    with the upload job into the download bucket and merge the upload metadata
    into the appropriate clinical trial JSON.
    """
    job_id = int(extract_pubsub_data(event))

    with sqlalchemy_session() as session:
        job: AssayUploads = AssayUploads.find_by_id(job_id, session=session)

        print("Detected completed upload job for user %s" % job.uploader_email)

        trial_id_field = "lead_organization_study_id"
        if not trial_id_field in job.metadata:
            # We should never hit this. This function should only be called on pre-validated metadata.
            raise Exception(
                "Invalid metadata: cannot find study ID in metadata. Aborting ingestion."
            )
        trial_id = job.metadata[trial_id_field]

        url_mapping = {}
        metadata_with_urls = job.metadata
        downloadable_files = []
        for upload_url, target_url, uuid in job.upload_uris_with_data_uris_with_uuids():

            url_mapping[upload_url] = target_url

            # Copy the uploaded GCS object to the data bucket
            metadata_with_urls, artifact_metadata = _copy_gcs_object_and_update_metadata(
                metadata_with_urls,
                job.assay_type,
                uuid,
                GOOGLE_UPLOAD_BUCKET,
                upload_url,
                GOOGLE_DATA_BUCKET,
                target_url,
            )

            # Hang on to the artifact metadata
            downloadable_files.append(artifact_metadata)

        # Add metadata for this upload to the database
        print("Merging metadata from upload %d into trial %s" % (job.id, trial_id))
        TrialMetadata.patch_trial_metadata(
            trial_id, metadata_with_urls, session=session
        )

        # Save downloadable files to the database
        # NOTE: this needs to happen after TrialMetadata.patch_trial_metadata
        # in order to avoid violating a foreign-key constraint on the trial_id
        # in the event that this is the first upload for a trial.
        for artifact_metadata in downloadable_files:
            print(f"Saving metadata for {target_url} to downloadable_files table.")
            DownloadableFiles.create_from_metadata(
                trial_id, job.assay_type, artifact_metadata, session=session
            )

    # Google won't actually do anything with this response; it's
    # provided for testing purposes only.
    return jsonify(url_mapping)


def _gcs_copy(
    source_bucket: str, source_object: str, target_bucket: str, target_object: str
):
    """Copy a GCS object from one bucket to another"""
    print(
        f"Copying gs://{source_bucket}/{source_object} to gs://{target_bucket}/{target_object}"
    )
    storage_client = storage.Client()
    from_bucket = storage_client.get_bucket(source_bucket)
    from_object = from_bucket.blob(source_object)
    to_bucket = storage_client.get_bucket(target_bucket)
    to_object = from_bucket.copy_blob(from_object, to_bucket, new_name=target_object)
    return to_object


def _copy_gcs_object_and_update_metadata(
    metadata: dict,
    assay_type: str,
    UUID: str,
    source_bucket: str,
    source_object: str,
    target_bucket: str,
    target_object: str,
) -> (dict, dict):
    """Copy a GCS object from one bucket to another and add the GCS uri to the provided metadata."""

    to_object = _gcs_copy(source_bucket, source_object, target_bucket, target_object)

    print(f"Adding artifact {to_object.name} to metadata.")
    updated_trial_metadata, artifact_metadata = TrialMetadata.merge_gcs_artifact(
        metadata, assay_type, UUID, to_object
    )

    return updated_trial_metadata, artifact_metadata
