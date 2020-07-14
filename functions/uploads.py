"""A pub/sub triggered functions that respond to data upload events"""
from multiprocessing.pool import ThreadPool
from contextlib import contextmanager
from typing import Optional, Tuple
from os import environ
from datetime import datetime, timedelta

from flask import jsonify
from google.cloud import storage
from cidc_api.models import (
    UploadJobs,
    TrialMetadata,
    DownloadableFiles,
    UploadJobStatus,
    prism,
)
from cidc_api.shared.gcloud_client import publish_artifact_upload, _encode_and_publish

from .settings import (
    FLASK_ENV,
    GOOGLE_DATA_BUCKET,
    GOOGLE_UPLOAD_BUCKET,
    GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT,
    GOOGLE_ANALYSIS_GROUP_ROLE,
    GOOGLE_ASSAY_OR_ANALYSIS_UPLOAD_TOPIC,
    GOOGLE_ANALYSIS_PERMISSIONS_GRANT_FOR_DAYS,
)
from .util import (
    BackgroundContext,
    extract_pubsub_data,
    sqlalchemy_session,
    make_pseudo_blob,
)


@contextmanager
def saved_failure_status(job: UploadJobs, session):
    """Save an upload failure to the database before raising an exception."""
    try:
        yield
    except Exception as e:
        job.status = UploadJobStatus.MERGE_FAILED.value
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
        job: UploadJobs = UploadJobs.find_by_id(job_id, session=session)
        if not job:
            raise Exception(f"No assay upload job with id {job_id} found.")

        # Ensure this is a completed upload and nothing else
        if UploadJobStatus(job.status) != UploadJobStatus.UPLOAD_COMPLETED:
            raise Exception(
                f"Received ID for job with status {job.status}. Aborting ingestion."
            )

        print(
            f"Detected completed upload job (job_id={job_id}) for user {job.uploader_email}"
        )

        trial_id = job.metadata_patch.get(prism.PROTOCOL_ID_FIELD_NAME)
        if not trial_id:
            # We should never hit this, since metadata should be pre-validated.
            with saved_failure_status(job, session):
                raise Exception(
                    f"Invalid assay metadata: missing protocol identifier ({prism.PROTOCOL_ID_FIELD_NAME})."
                )

        def do_copy(urls):
            upload_url, target_url = urls
            with saved_failure_status(job, session):
                # Copy the uploaded GCS object to the data bucket
                destination_object = _gcs_copy(
                    GOOGLE_UPLOAD_BUCKET, upload_url, GOOGLE_DATA_BUCKET, target_url
                )
            return destination_object

        uuids = []
        url_mapping = []
        for upload_url, target_url, uuid in job.upload_uris_with_data_uris_with_uuids():
            url_mapping.append((upload_url, target_url))
            uuids.append(uuid)

        # Copy GCS blobs in parallel
        pool = ThreadPool(8)
        destination_objects = pool.map(do_copy, url_mapping)
        pool.close()

        downloadable_files = []
        metadata_with_urls = job.metadata_patch
        for destination_object, uuid in zip(destination_objects, uuids):
            with saved_failure_status(job, session):
                # Add the artifact info to the metadata patch
                print(f"Adding artifact {destination_object.name} to metadata.")
                (
                    metadata_with_urls,
                    artifact_metadata,
                    additional_metadata,
                ) = TrialMetadata.merge_gcs_artifact(
                    metadata_with_urls, job.upload_type, uuid, destination_object
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
            trial = TrialMetadata.patch_assays(
                trial_id, metadata_with_urls, session=session
            )

        # Save downloadable files to the database
        # NOTE: this needs to happen after TrialMetadata.patch_assays
        # in order to avoid violating a foreign-key constraint on the trial_id
        # in the event that this is the first upload for a trial.
        for artifact_metadata, additional_metadata in downloadable_files:
            print(f"Saving metadata to downloadable_files table: {artifact_metadata}")
            with saved_failure_status(job, session):
                DownloadableFiles.create_from_metadata(
                    trial_id,
                    job.upload_type,
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
                trial_id, job.upload_type, "Assay Metadata", xlsx_blob, session=session
            )

        # Update the job metadata to include artifacts
        job.metadata_patch = metadata_with_urls

        # Making files downloadable by a specified biofx analysis team group
        assay_prefix = job.upload_type.split("_")[0]  # 'wes_bam' -> 'wes'
        if assay_prefix in GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT:
            analysis_group_email = GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT[assay_prefix]
            _gcs_add_prefix_reader_permission(
                analysis_group_email,  # to whom give access to
                f"{trial_id}/{assay_prefix}",  # to what sub-folder
            )

        # Save the upload success and trigger email alert if transaction succeeds
        job.ingestion_success(trial, session=session, send_email=True, commit=True)

        # Trigger post-processing on uploaded data files
        for _, target_url in url_mapping:
            print(f"Publishing file object URL {target_url} to 'artifact_upload' topic")
            publish_artifact_upload(target_url)

        # Trigger post-processing on entire upload
        _encode_and_publish(str(job.id), GOOGLE_ASSAY_OR_ANALYSIS_UPLOAD_TOPIC).result()

    # Google won't actually do anything with this response; it's
    # provided for testing purposes only.
    return jsonify(dict(url_mapping))


def _gcs_add_prefix_reader_permission(group_email: str, prefix: str):
    """
    Adds a conditional policy to GCS bucket (default: GOOGLE_DATA_BUCKET)
    that allows `group_email` to read all objects within a `prefix`.
    """
    print(
        f"Adding {group_email} {GOOGLE_ANALYSIS_GROUP_ROLE} access to GCS {GOOGLE_DATA_BUCKET} policy"
    )

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GOOGLE_DATA_BUCKET)

    # get v3 policy to use condition in bindings
    policy = bucket.get_iam_policy(requested_policy_version=3)

    # Set the policy's version to 3 to use condition in bindings.
    policy.version = 3

    cleaned_prefix = prefix.replace('"', '\\"').lstrip("/")
    grant_until_date = (
        (datetime.now() + timedelta(GOOGLE_ANALYSIS_PERMISSIONS_GRANT_FOR_DAYS))
        .date()
        .isoformat()
    )

    prefixCheck = f'resource.name.startsWith("projects/_/buckets/{GOOGLE_DATA_BUCKET}/objects/{cleaned_prefix}/")'
    expiryCheck = f'request.time < timestamp("{grant_until_date}T00:00:00Z")'

    # following https://github.com/GoogleCloudPlatform/python-docs-samples/pull/2730/files
    policy.bindings.append(
        {
            "role": GOOGLE_ANALYSIS_GROUP_ROLE,
            "members": ["group:" + group_email],
            "condition": {
                "title": f"Biofx {prefix} until {grant_until_date}",
                "description": f"Auto-assigned from cidc-cloud-functions/uploads on {datetime.now()}",
                "expression": f"{prefixCheck} && {expiryCheck}",
            },
        }
    )

    bucket.set_iam_policy(policy)


def _gcs_copy(
    source_bucket: str, source_object: str, target_bucket: str, target_object: str
):
    """Copy a GCS object from one bucket to another"""
    if FLASK_ENV == "development":
        print(
            f"Would've copied gs://{source_bucket}/{source_object} gs://{target_bucket}/{target_object}"
        )
        return make_pseudo_blob(target_object)

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

    if FLASK_ENV == "development":
        print(
            f"Getting local {object_name} instead of gs://{bucket_name}/{object_name}"
        )
        return (bucket_name, make_pseudo_blob(object_name))

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(object_name) if object_name else None
    return bucket, blob
