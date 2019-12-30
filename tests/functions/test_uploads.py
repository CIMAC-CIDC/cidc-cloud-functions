from unittest.mock import MagicMock
from collections import namedtuple
import datetime

import pytest

from cidc_api.models import (
    AssayUploads,
    AssayUploadStatus,
    TrialMetadata,
    DownloadableFiles,
    prism,
)

from functions import uploads
from functions.uploads import ingest_upload, saved_failure_status
from functions.settings import GOOGLE_DATA_BUCKET

from tests.util import make_pubsub_event, with_app_context

JOB_ID = 1
URI1 = "/path/to/file1"
URI2 = "/path/to/deeper/file2"
UPLOAD_DATE_PATH = "/2019-09-04T17:00:28.685967"
FILE_MAP = {URI1 + UPLOAD_DATE_PATH: "uuid1", URI2 + UPLOAD_DATE_PATH: "uuid2"}


def email_was_sent(stdout: str) -> bool:
    return "Would send email with subject '[UPLOAD SUCCESS]" in stdout


_gcs_obj_mock = namedtuple(
    "gsc_object_mock", ["name", "size", "time_created", "md5_hash", "crc32c"]
)


@with_app_context
def test_ingest_upload(capsys, monkeypatch):
    """Test upload data transfer functionality"""

    TS_AND_PATH = "/1234/local_path1.txt"
    ARTIFACT = {"test-prop": "test-val"}
    TRIAL_ID = "CIMAC-12345"

    job = AssayUploads(
        id=JOB_ID,
        uploader_email="test@email.com",
        trial_id=TRIAL_ID,
        gcs_xlsx_uri="test.xlsx",
        gcs_file_map=FILE_MAP,
        assay_patch={
            prism.PROTOCOL_ID_FIELD_NAME: TRIAL_ID,
            "assays": {
                "wes": [
                    {
                        "records": [
                            {
                                "cimac_id": "CIMAC-mock-sa-id",
                                "files": {
                                    "r1": {"upload_placeholder": "uuid1"},
                                    "r2": {"upload_placeholder": "uuid2"},
                                },
                            }
                        ]
                    }
                ]
            },
        },
        status=AssayUploadStatus.UPLOAD_COMPLETED.value,
        assay_type="wes_bam",
    )

    # Since the test database isn't yet set up with migrations,
    # it won't have the correct relations in it, so we can't actually
    # store or retrieve data
    find_by_id = MagicMock()
    find_by_id.return_value = job
    monkeypatch.setattr(AssayUploads, "find_by_id", find_by_id)

    # Mock data transfer functionality
    _gcs_copy = MagicMock()
    _gcs_copy.side_effect = lambda source_bucket, source_object, target_bucket, target_object: _gcs_obj_mock(
        target_object,
        100,
        datetime.datetime.now(),
        "gsc_url_mock_md5",
        "gsc_url_mock_crc32c",
    )
    monkeypatch.setattr("functions.uploads._gcs_copy", _gcs_copy)

    _get_bucket_and_blob = MagicMock()
    xlsx_blob = MagicMock()
    _get_bucket_and_blob.return_value = None, xlsx_blob
    monkeypatch.setattr("functions.uploads._get_bucket_and_blob", _get_bucket_and_blob)

    monkeypatch.setattr(
        "functions.uploads.GOOGLE_ANALYSIS_PERMISSIONS_GROUPS_DICT",
        {"wes": "analysis-group@email"},
    )

    _set_iam_policy = MagicMock()

    # def _f(a):
    #     raise Exception(a.to_api_repr())

    # _set_iam_policy.side_effect = _f

    monkeypatch.setattr(
        "google.cloud.storage.bucket.Bucket.set_iam_policy", _set_iam_policy
    )

    # Mock metadata merging functionality
    _save_file = MagicMock()
    monkeypatch.setattr(DownloadableFiles, "create_from_metadata", _save_file)

    _save_blob_file = MagicMock()
    monkeypatch.setattr(DownloadableFiles, "create_from_blob", _save_blob_file)

    _merge_metadata = MagicMock()
    monkeypatch.setattr(TrialMetadata, "patch_assays", _merge_metadata)

    publish_artifact_upload = MagicMock()
    monkeypatch.setattr(uploads, "publish_artifact_upload", publish_artifact_upload)

    successful_upload_event = make_pubsub_event(str(job.id))
    response = ingest_upload(successful_upload_event, None).json

    assert response[URI1 + UPLOAD_DATE_PATH] == URI1
    assert response[URI2 + UPLOAD_DATE_PATH] == URI2
    find_by_id.assert_called_once()
    # Check that we copied multiple objects
    _gcs_copy.assert_called() and not _gcs_copy.assert_called_once()
    # Check that we tried to save multiple files
    _save_file.assert_called() and not _save_file.assert_called_once()
    # Check that we tried to merge metadata once
    _merge_metadata.assert_called_once()
    # Check that we got the xlsx blob metadata from GCS
    _get_bucket_and_blob.assert_called_with(GOOGLE_DATA_BUCKET, job.gcs_xlsx_uri)
    # Check that we created a downloadable file for the xlsx file blob
    assert _save_blob_file.call_args[:-1][0] == (
        "CIMAC-12345",
        "wes_bam",
        "Assay Metadata",
        xlsx_blob,
    )
    # Check that we tried to update GCS access policy
    _set_iam_policy.assert_called_once()
    # Check that we aded GCS access for biofx team
    policy = _set_iam_policy.call_args[0][0].to_api_repr()
    assert ("group:analysis-group@email", "roles/storage.legacyObjectReader") in [
        (b["members"][0], b["role"]) for b in policy["bindings"]
    ]

    # Check that the job status was updated to reflect a successful upload
    assert job.status == AssayUploadStatus.MERGE_COMPLETED.value
    assert email_was_sent(capsys.readouterr()[0])
    publish_artifact_upload.assert_called()


def test_saved_failure_status(capsys):
    """Check that the saved_failure_status context manager does what it claims."""
    session = MagicMock()
    job = MagicMock()
    job.status = None
    job.status_details = None

    # Non-raising code
    with saved_failure_status(job, session):
        print("not failing!")

    assert job.status is None and job.status_details is None
    session.commit.assert_not_called()

    # Raising code
    message = "uh oh!"
    with pytest.raises(Exception, match=message):
        with saved_failure_status(job, session):
            raise Exception(message)

    assert job.status == AssayUploadStatus.MERGE_FAILED.value
    assert job.status_details == message
    session.commit.assert_called_once()

    assert not email_was_sent(capsys.readouterr()[0])
