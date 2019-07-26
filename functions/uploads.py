"""A pub/sub triggered functions that respond to data upload events"""
import base64

from .util import BackgroundContext, extract_pubsub_data


def ingest_upload(event: dict, context: BackgroundContext):
    """
    When a successful upload event is published, move the data associated
    with the upload job into the download bucket and merge the upload metadata
    into the appropriate clinical trial JSON.

    TODO: actually implement the above functionality. Right now, the function
    just logs the ID of the upload job.
    """
    job_id = extract_pubsub_data(event)
    print(f"Received upload success event for Job {job_id}")

