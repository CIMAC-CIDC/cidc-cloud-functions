"""A pub/sub triggered functions that respond to data upload events"""
from flask import jsonify
from google.cloud import storage
from cidc_api.models import UploadJobs

from .settings import GOOGLE_DATA_BUCKET, GOOGLE_UPLOAD_BUCKET
from .util import BackgroundContext, extract_pubsub_data, get_db_session


def ingest_upload(event: dict, context: BackgroundContext):
    """
    When a successful upload event is published, move the data associated
    with the upload job into the download bucket and merge the upload metadata
    into the appropriate clinical trial JSON.

    TODO: actually implement the above functionality.
    """
    job_id = int(extract_pubsub_data(event))
    session = get_db_session()

    job: UploadJobs = UploadJobs.find_by_id(job_id, session=session)

    print("Detected completed upload job for user %s" % job.uploader_email)

    # TODO: trail id extraction should be owned by cidc_schemas.prism
    study_id_field = "lead_organization_study_id"
    if not study_id_field in job.metadata_json_patch:
        # TODO: improve this error reporting...
        raise Exception("Cannot find study ID in metadata. Ingestion impossible.")

    # TODO: actually merge the metadata into the clinical trial JSON
    study_id = job.metadata_json_patch[study_id_field]
    print(
        "(DRY RUN) merging metadata from upload %d into trial %s" % (job.id, study_id)
    )

    url_mapping = {}
    for upload_url in job.gcs_file_uris:
        # We expected URIs in the upload bucket to have a structure like
        # [trial id]/[patient id]/[sample id]/[aliquot id]/[timestamp]/[local file].
        # We strip off the /[timestamp]/[local file] suffix from the upload url,
        # since we don't care when this was uploaded or where from on the uploader's
        # computer.
        target_url = "/".join(upload_url.split("/")[:-2])
        url_mapping[upload_url] = target_url

        # Copy the uploaded GCS object to the data bucket
        _copy_gcs_object(
            GOOGLE_UPLOAD_BUCKET, upload_url, GOOGLE_DATA_BUCKET, target_url
        )

    # Google won't actually do anything with this response; it's
    # provided for testing purposes only.
    return jsonify(url_mapping)


def _copy_gcs_object(source_bucket, source_object, target_bucket, target_object):
    """Copy a GCS object from one bucket to another."""
    print(
        f"Copying gs://{source_bucket}/{source_object} to gs://{target_bucket}/{target_object}"
    )
    storage_client = storage.Client()
    from_bucket = storage_client.get_bucket(source_bucket)
    from_object = from_bucket.blob(source_object)
    to_bucket = storage_client.get_bucket(target_bucket)
    to_object = from_bucket.copy_blob(from_object, to_bucket, new_name=target_object)
    print(
        f"Copied gs://{from_bucket.name}/{from_object.name} to gs://{to_bucket.name}/{to_object.name}"
    )
