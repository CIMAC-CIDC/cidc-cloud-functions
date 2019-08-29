from unittest.mock import MagicMock

from cidc_api.models import UploadJobs, TrialMetadata, DownloadableFiles

from tests.util import make_pubsub_event, with_app_context
from functions.uploads import ingest_upload


@with_app_context
def test_ingest_upload(db_session, monkeypatch):
    """Test upload data transfer functionality"""

    JOB_ID = 1
    URI1 = "/path/to/file1"
    URI2 = "/path/to/deeper/file2"
    TS_AND_PATH = "/1234/local_path1.txt"
    FILE_URIS = [URI1 + TS_AND_PATH, URI2 + TS_AND_PATH]
    ARTIFACT = {"test-prop": "test-val"}

    job = UploadJobs(
        id=JOB_ID,
        uploader_email="test@email.com",
        gcs_file_uris=FILE_URIS,
        metadata_json_patch={"lead_organization_study_id": "CIMAC-12345"},
        status="completed",
        assay_type="wes"
    )

    # Since the test database isn't yet set up with migrations,
    # it won't have the correct relations in it, so we can't actually
    # store or retrieve data
    find_by_id = MagicMock()
    find_by_id.return_value = job
    monkeypatch.setattr(UploadJobs, "find_by_id", find_by_id)

    # Mock data transfer functionality
    _copy_gcs_object = MagicMock()
    _copy_gcs_object.return_value = job.metadata_json_patch, ARTIFACT
    monkeypatch.setattr(
        "functions.uploads._copy_gcs_object_and_update_metadata", _copy_gcs_object
    )

    # Mock metadata merging functionality
    _save_file = MagicMock()
    monkeypatch.setattr(DownloadableFiles, "create_from_metadata", _save_file)

    _merge_metadata = MagicMock()
    monkeypatch.setattr(TrialMetadata, "patch_trial_metadata", _merge_metadata)

    successful_upload_event = make_pubsub_event(str(job.id))
    response = ingest_upload(successful_upload_event, None)

    assert response.json[URI1 + TS_AND_PATH] == URI1
    assert response.json[URI2 + TS_AND_PATH] == URI2
    find_by_id.assert_called_once_with(JOB_ID, session=db_session)
    # Check that we copied multiple objects
    _copy_gcs_object.assert_called() and not _copy_gcs_object.assert_called_once()
    # Check that we tried to save multiple files
    _save_file.assert_called() and not _save_file.assert_called_once()
    # Check that we tried to merge metadata once
    _merge_metadata.assert_called_once()
