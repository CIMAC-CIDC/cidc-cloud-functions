from tests.util import make_pubsub_event
from functions.uploads import ingest_upload


def test_ingest_upload():
    """Test stub event-processing functionality"""
    job_id = "1"
    successful_upload_event = make_pubsub_event(job_id)
    ingest_upload(successful_upload_event, None)
