from cidc_api.models import UploadJobs

from tests.util import make_pubsub_event, with_app_context
from functions.uploads import ingest_upload


@with_app_context
def test_ingest_upload(db_session):
    """Test upload data transfer functionality"""

    EMAIL = "test@email.com"
    URI1 = "/path/to/file1"
    URI2 = "/path/to/deeper/file2"
    TS_AND_PATH = "/1234/local_path1.txt"
    FILE_URIS = [URI1 + TS_AND_PATH, URI2 + TS_AND_PATH]
    METADATA_PATCH = {"lead_organization_study_id": "CIMAC-12345"}

    # Add a test job to the database
    job = UploadJobs.create(EMAIL, FILE_URIS, METADATA_PATCH, session=db_session)

    successful_upload_event = make_pubsub_event(str(job.id))
    response = ingest_upload(successful_upload_event, None)

    assert response.json[URI1 + TS_AND_PATH] == URI1
    assert response.json[URI2 + TS_AND_PATH] == URI2
