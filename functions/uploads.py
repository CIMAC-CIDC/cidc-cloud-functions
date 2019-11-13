"""A pub/sub triggered functions that respond to data upload events"""
from contextlib import contextmanager
from typing import Optional, Tuple
from os import environ
from collections import namedtuple
from datetime import datetime

from flask import jsonify
from google.cloud import storage
from cidc_api.models import (
    AssayUploads,
    TrialMetadata,
    DownloadableFiles,
    AssayUploadStatus,
    prism,
)

from .settings import GOOGLE_DATA_BUCKET, GOOGLE_UPLOAD_BUCKET
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session


@contextmanager
def saved_failure_status(job: AssayUploads, session):
    """Save an upload failure to the database before raising an exception."""
    try:
        yield
    except Exception as e:
        job.status = AssayUploadStatus.MERGE_FAILED.value
        job.status_details = str(e)
        session.commit()
        raise e


def ingest_upload(event: dict, context: BackgroundContext):
    """
    When a successful upload event is published, move the data associated
    with the upload job into the download bucket and merge the upload metadata
    into the appropriate clinical trial JSON.
    """
    job_id = int(extract_pubsub_data(event))

    with sqlalchemy_session() as session:
        job: AssayUploads = AssayUploads.find_by_id(job_id, session=session)
        if not job:
            raise Exception(f"No assay upload job with id {job_id} found.")

        # Ensure this is a completed upload and nothing else
        if AssayUploadStatus(job.status) != AssayUploadStatus.UPLOAD_COMPLETED:
            raise Exception(
                f"Received ID for job with status {job.status}. Aborting ingestion."
            )

        print(
            f"Detected completed upload job (job_id={job_id}) for user {job.uploader_email}"
        )

        trial_id = job.assay_patch.get(prism.PROTOCOL_ID_FIELD_NAME)
        if not trial_id:
            # We should never hit this, since metadata should be pre-validated.
            with saved_failure_status(job, session):
                raise Exception(
                    f"Invalid assay metadata: missing protocol identifier ({prism.PROTOCOL_ID_FIELD_NAME})."
                )

        url_mapping = {}
        metadata_with_urls = job.assay_patch
        downloadable_files = []
        for upload_url, target_url, uuid in job.upload_uris_with_data_uris_with_uuids():

            url_mapping[upload_url] = target_url

            with saved_failure_status(job, session):
                # Copy the uploaded GCS object to the data bucket
                destination_object = _gcs_copy(
                    GOOGLE_UPLOAD_BUCKET, upload_url, GOOGLE_DATA_BUCKET, target_url
                )
                # Add the artifact info to the metadata patch
                print(f"Adding artifact {destination_object.name} to metadata.")
                metadata_with_urls, artifact_metadata, additional_metadata = TrialMetadata.merge_gcs_artifact(
                    metadata_with_urls, job.assay_type, uuid, destination_object
                )

            # Hang on to the artifact metadata
            print(f"artifact metadata: {artifact_metadata}")
            downloadable_files.append((artifact_metadata, additional_metadata))

        # Add metadata for this upload to the database
        print(
            "Merging metadata from upload %d into trial %s: " % (job.id, trial_id),
            metadata_with_urls,
        )
        with saved_failure_status(job, session):
            TrialMetadata.patch_assays(trial_id, metadata_with_urls, session=session)

        # Save downloadable files to the database
        # NOTE: this needs to happen after TrialMetadata.patch_assays
        # in order to avoid violating a foreign-key constraint on the trial_id
        # in the event that this is the first upload for a trial.
        for artifact_metadata, additional_metadata in downloadable_files:
            print(f"Saving metadata to downloadable_files table: {artifact_metadata}")
            with saved_failure_status(job, session):
                DownloadableFiles.create_from_metadata(
                    trial_id,
                    job.assay_type,
                    artifact_metadata,
                    additional_metadata=additional_metadata,
                    session=session,
                    commit=False,
                )

        # Additionally, make the metadata xlsx a downloadable file
        with saved_failure_status(job, session):
            _, xlsx_blob = _get_bucket_and_blob(GOOGLE_DATA_BUCKET, job.gcs_xlsx_uri)
            full_uri = f"gs://{GOOGLE_DATA_BUCKET}/{xlsx_blob.name}"
            print(f"Saving {full_uri} as a downloadable_file.")
            DownloadableFiles.create_from_blob(
                trial_id, job.assay_type, "Assay Metadata", xlsx_blob, session=session
            )

        # Save the upload success and trigger email alert
        job.ingestion_success(session=session, send_email=True)
        session.commit()

    # Google won't actually do anything with this response; it's
    # provided for testing purposes only.
    return jsonify(url_mapping)


_pseudo_blob = namedtuple("_pseudo_blob", ["name", "size", "md5_hash", "time_created"])


def _gcs_copy(
    source_bucket: str, source_object: str, target_bucket: str, target_object: str
):
    """Copy a GCS object from one bucket to another"""
    if environ.get("FLASK_ENV") == "development":
        print(
            f"Would've copied {source_bucket}/{source_object} {target_bucket}/{target_object}"
        )
        return _pseudo_blob(
            f"{target_bucket}/{target_object}", 0, "_pseudo_md5_hash", datetime.now()
        )

    print(
        f"Copying gs://{source_bucket}/{source_object} to gs://{target_bucket}/{target_object}"
    )
    from_bucket, from_object = _get_bucket_and_blob(source_bucket, source_object)
    to_bucket, _ = _get_bucket_and_blob(target_bucket, None)
    to_object = from_bucket.copy_blob(from_object, to_bucket, new_name=target_object)

    # We want to maintain the actual upload time of this object, which is the moment
    # it was originally created in the upload bucket, not the moment it was moved
    # to the data bucket.
    # NOTE: this uses implementation details not exposed in the public api of `storage.Blob`.
    # As such, this may break if we decide to update the google-cloud-storage package.
    fmted_time = from_object.time_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    to_object._properties["timeCreated"] = fmted_time

    return to_object


def _get_bucket_and_blob(
    bucket_name: str, object_name: Optional[str]
) -> Tuple[storage.Bucket, Optional[storage.Blob]]:
    """Get GCS metadata for a storage bucket and blob"""

    if environ.get("FLASK_ENV") == "development":
        return (
            bucket_name,
            _pseudo_blob(
                f"{bucket_name}/{object_name}", 0, "_pseudo_md5_hash", datetime.now()
            ),
        )

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(object_name) if object_name else None
    return bucket, blob
