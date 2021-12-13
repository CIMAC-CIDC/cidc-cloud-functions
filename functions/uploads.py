"""A pub/sub triggered functions that respond to data upload events"""
import sys
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Optional, Tuple, NamedTuple
from datetime import datetime, timedelta

from .settings import (
    ENV,
    GOOGLE_ACL_DATA_BUCKET,
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

from flask import jsonify
from google.cloud import storage
from cidc_api.models import (
    DownloadableFiles,
    Permissions,
    prism,
    TrialMetadata,
    UploadJobs,
    UploadJobStatus,
)
from cidc_api.shared.gcloud_client import publish_artifact_upload, _encode_and_publish

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)

THREADPOOL_THREADS = 16


class URLBundle(NamedTuple):
    upload_url: str
    target_url: str
    artifact_uuid: str


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

    Handles (expiring) IAM permissions role `CIDC_biofx` separately from the main API system.
    See also: https://github.com/CIMAC-CIDC/cidc-api-gae/blob/fee3b303b397272bbd500289ea64976c5a510b27/cidc_api/models/models.py#L460
    """
    storage_client = storage.Client()

    job_id = int(extract_pubsub_data(event))

    logger.info(f"ingest_upload execution started on upload job id {job_id}")

    with sqlalchemy_session() as session:
        job: UploadJobs = UploadJobs.find_by_id(job_id, session=session)

        # Check ingestion pre-conditions
        if not job:
            raise Exception(f"No assay upload job with id {job_id} found.")
        if UploadJobStatus(job.status) != UploadJobStatus.UPLOAD_COMPLETED:
            raise Exception(
                f"Received ID for job with status {job.status}. Aborting ingestion."
            )
        trial_id = job.metadata_patch.get(prism.PROTOCOL_ID_FIELD_NAME)
        if not trial_id:
            # We should never hit this, since metadata should be pre-validated.
            with saved_failure_status(job, session):
                raise Exception(
                    f"Invalid assay metadata: missing protocol identifier ({prism.PROTOCOL_ID_FIELD_NAME})."
                )

        logger.info(
            f"Found completed upload job (job_id={job_id}) with uploader {job.uploader_email}"
        )

        url_bundles = [
            URLBundle(*bundle) for bundle in job.upload_uris_with_data_uris_with_uuids()
        ]

        # Copy GCS blobs in parallel
        logger.info("Copying artifacts from upload bucket to data bucket.")
        with ThreadPoolExecutor(THREADPOOL_THREADS) as executor, saved_failure_status(
            job, session
        ):
            destination_objects = executor.map(
                lambda url_bundle: _gcs_copy(
                    storage_client,
                    GOOGLE_UPLOAD_BUCKET,
                    url_bundle.upload_url,
                    GOOGLE_ACL_DATA_BUCKET,
                    url_bundle.target_url,
                ),
                url_bundles,
            )

        metadata_patch = job.metadata_patch
        logger.info("Adding artifact metadata to metadata patch.")
        metadata_patch, downloadable_files = TrialMetadata.merge_gcs_artifacts(
            metadata_patch,
            job.upload_type,
            zip([ub.artifact_uuid for ub in url_bundles], destination_objects),
        )

        # Add metadata for this upload to the database
        logger.info(
            "Merging metadata from upload %d into trial %s: " % (job.id, trial_id),
            metadata_patch,
        )
        with saved_failure_status(job, session):
            trial = TrialMetadata.patch_assays(
                trial_id, metadata_patch, session=session
            )

        # Save downloadable files to the database
        # NOTE: this needs to happen after TrialMetadata.patch_assays
        # in order to avoid violating a foreign-key constraint on the trial_id
        # in the event that this is the first upload for a trial.
        logger.info("Saving artifact records to the downloadable_files table.")
        for artifact_metadata, additional_metadata in downloadable_files:
            logger.debug(
                f"Saving metadata to downloadable_files table: {artifact_metadata}"
            )
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
            _, xlsx_blob = _get_bucket_and_blob(
                storage_client, GOOGLE_ACL_DATA_BUCKET, job.gcs_xlsx_uri
            )
            full_uri = f"gs://{GOOGLE_ACL_DATA_BUCKET}/{xlsx_blob.name}"
            data_format = "Assay Metadata"
            facet_group = f"{job.upload_type}|{data_format}"
            logger.info(f"Saving {full_uri} as a downloadable_file.")
            DownloadableFiles.create_from_blob(
                trial_id,
                job.upload_type,
                data_format,
                facet_group,
                xlsx_blob,
                session=session,
            )

        # Update the job metadata to include artifacts
        job.metadata_patch = metadata_patch

        # Making files downloadable by a specified biofx analysis team group
        # See also: https://github.com/CIMAC-CIDC/cidc-api-gae/blob/fee3b303b397272bbd500289ea64976c5a510b27/cidc_api/models/models.py#L460
        # # This is a separate permissions system from the main API that applies the expiring IAM role
        # # `CIDC_biofx` to the `cidc-dfci-biofx-[wes/rna]@ds` emails using a `trial/assay` prefix
        # # while removing any existing perm for the same prefix
        assay_prefix = job.upload_type.split("_")[0]  # 'wes_bam' -> 'wes'
        if assay_prefix in GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT:
            analysis_group_email = GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT[assay_prefix]
            _gcs_add_prefix_reader_permission(
                storage_client,
                analysis_group_email,  # to whom give access to
                f"{trial_id}/{assay_prefix}",  # to what sub-folder
            )

        # Save the upload success and trigger email alert if transaction succeeds
        job.ingestion_success(trial, session=session, send_email=True, commit=True)

        # Trigger post-processing on uploaded data files
        logger.info(f"Publishing object URLs to 'artifact_upload' topic")
        with ThreadPoolExecutor(THREADPOOL_THREADS) as executor:
            executor.map(
                lambda url_bundle: publish_artifact_upload(url_bundle.target_url),
                url_bundles,
            )

        # Trigger post-processing on entire upload
        report = _encode_and_publish(str(job.id), GOOGLE_ASSAY_OR_ANALYSIS_UPLOAD_TOPIC)
        if report:
            report.result()

        # Trigger download permissions for this upload job
        Permissions.grant_download_permissions_for_upload_job(job)

    # Google won't actually do anything with this response; it's
    # provided for testing purposes only.
    return jsonify(
        dict((bundle.upload_url, bundle.target_url) for bundle in url_bundles)
    )


def _gcs_add_prefix_reader_permission(
    storage_client: storage.Client, group_email: str, prefix: str
):
    """
    Adds a conditional policy to GCS bucket (default: GOOGLE_ACL_DATA_BUCKET)
    that allows `group_email` to read all objects within a `prefix`.
    """
    logger.info(
        f"Adding {group_email} {GOOGLE_ANALYSIS_GROUP_ROLE} access to GCS {GOOGLE_ACL_DATA_BUCKET} policy"
    )

    # get the bucket
    bucket = storage_client.get_bucket(GOOGLE_ACL_DATA_BUCKET)

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

    prefix_check = f'resource.name.startsWith("projects/_/buckets/{GOOGLE_ACL_DATA_BUCKET}/objects/{cleaned_prefix}/")'
    expiry_check = f'request.time < timestamp("{grant_until_date}T00:00:00Z")'
    group_member = f"group:{group_email}"

    # look for a duplicate conditional binding
    matching_binding_index = None
    for i, binding in enumerate(policy.bindings):
        role_matches = binding.get("role") == GOOGLE_ANALYSIS_GROUP_ROLE
        member_matches = group_member in binding.get("members", {})
        prefix_matches = prefix_check in binding.get("condition", {}).get(
            "expression", ""
        )
        if role_matches and member_matches and prefix_matches:
            # we shouldn't have multiple bindings matching these conditions
            if matching_binding_index is not None:
                warnings.warn(
                    f"Found multiple conditional bindings for {group_email} on {prefix}. This is an invariant violation - "
                    "check out permissions on the CIDC GCS buckets to debug."
                )
                break
            matching_binding_index = i

    # if one exists, delete it so we can replace it below
    if matching_binding_index is not None:
        policy.bindings.pop(matching_binding_index)

    # following https://github.com/GoogleCloudPlatform/python-docs-samples/pull/2730/files
    policy.bindings.append(
        {
            "role": GOOGLE_ANALYSIS_GROUP_ROLE,
            "members": {group_member},
            "condition": {
                "title": f"Biofx {prefix} until {grant_until_date}",
                "description": f"Auto-assigned from cidc-cloud-functions/uploads on {datetime.now()}",
                "expression": f"{prefix_check} && {expiry_check}",
            },
        }
    )

    bucket.set_iam_policy(policy)


def _gcs_copy(
    storage_client: storage.Client,
    source_bucket: str,
    source_object: str,
    target_bucket: str,
    target_object: str,
):
    """Copy a GCS object from one bucket to another"""
    if ENV == "dev":
        logger.debug(
            f"Would've copied gs://{source_bucket}/{source_object} gs://{target_bucket}/{target_object}"
        )
        return make_pseudo_blob(target_object)

    logger.debug(
        f"Copying gs://{source_bucket}/{source_object} to gs://{target_bucket}/{target_object}"
    )
    from_bucket, from_object = _get_bucket_and_blob(
        storage_client, source_bucket, source_object
    )
    if from_object is None:
        raise Exception(f"Couldn't get the GCS blob to copy: {source_object}")
    to_bucket, _ = _get_bucket_and_blob(storage_client, target_bucket, None)
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
    storage_client: storage.Client, bucket_name: str, object_name: Optional[str]
) -> Tuple[storage.Bucket, Optional[storage.Blob]]:
    """Get GCS metadata for a storage bucket and blob"""

    if ENV == "dev":
        logger.debug(
            f"Getting local {object_name} instead of gs://{bucket_name}/{object_name}"
        )
        return (bucket_name, make_pseudo_blob(object_name))

    # get the bucket.
    bucket = storage_client.get_bucket(bucket_name)

    # get the blob and return it
    blob = bucket.get_blob(object_name) if object_name else None
    return bucket, blob
