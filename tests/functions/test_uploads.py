from unittest.mock import MagicMock
from collections import namedtuple
import datetime

from cidc_api.models import AssayUploads, TrialMetadata, DownloadableFiles

from tests.util import make_pubsub_event, with_app_context
from functions.uploads import ingest_upload


@with_app_context
def test_ingest_upload(monkeypatch):
    """Test upload data transfer functionality"""

    JOB_ID = 1
    URI1 = "/path/to/file1"
    URI2 = "/path/to/deeper/file2"
    TS_AND_PATH = "/1234/local_path1.txt"
    UPLOAD_DATE_PATH = '/2019-09-04T17:00:28.685967'
    FILE_MAP = {
        URI1+UPLOAD_DATE_PATH : "uuid1", 
        URI2+UPLOAD_DATE_PATH : "uuid2"
    }
    ARTIFACT = {"test-prop": "test-val"}

    job = AssayUploads(
        id=JOB_ID,
        uploader_email="test@email.com",
        gcs_file_map=FILE_MAP,
        assay_patch={
            "lead_organization_study_id": "CIMAC-12345",
            "assays": {
                "wes": [
                    {
                        "records": [
                            {
                                "cimac_participant_id": "CIMAC-mock-pa-id",
                                "cimac_sample_id": "CIMAC-mock-sa-id",
                                "cimac_aliquot_id": "CIMAC-mock-al-id",
                                "files": {"fastq_1": {}},
                            }
                        ]
                    }
                ]
            },
        },
        status="completed",
        assay_type="wes",
    )

    # Since the test database isn't yet set up with migrations,
    # it won't have the correct relations in it, so we can't actually
    # store or retrieve data
    find_by_id = MagicMock()
    find_by_id.return_value = job
    monkeypatch.setattr(AssayUploads, "find_by_id", find_by_id)

    # Mock data transfer functionality
    _gcs_copy = MagicMock()
    _gcs_copy.return_value = namedtuple(
        "gsc_object_mock", ["name", "size", "time_created", "md5_hash"]
    )(
        "CIMAC-mock-pa-id/CIMAC-mock-sa-id/CIMAC-mock-al-id/wes/fastq_1",
        100,
        datetime.datetime.now(),
        "gsc_url_mock_hash",
    )
    monkeypatch.setattr("functions.uploads._gcs_copy", _gcs_copy)

    # Mock metadata merging functionality
    _save_file = MagicMock()
    monkeypatch.setattr(DownloadableFiles, "create_from_metadata", _save_file)

    _merge_metadata = MagicMock()
    monkeypatch.setattr(TrialMetadata, "patch_trial_metadata", _merge_metadata)

    successful_upload_event = make_pubsub_event(str(job.id))
    response = ingest_upload(successful_upload_event, None).json

    assert response[URI1+UPLOAD_DATE_PATH] == URI1
    assert response[URI2+UPLOAD_DATE_PATH] == URI2
    find_by_id.assert_called_once()
    # Check that we copied multiple objects
    _gcs_copy.assert_called() and not _gcs_copy.assert_called_once()
    # Check that we tried to save multiple files
    _save_file.assert_called() and not _save_file.assert_called_once()
    # Check that we tried to merge metadata once
    _merge_metadata.assert_called_once()
