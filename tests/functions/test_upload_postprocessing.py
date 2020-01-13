from unittest.mock import MagicMock

import pytest
from cidc_api.models import AssayUploads, AssayUploadStatus

from functions import upload_postprocessing

from tests.util import make_pubsub_event

event = make_pubsub_event("1")


def test_manifest_preconditions(monkeypatch):
    """Ensure derive_files_from_manifest_upload blocks derivation under the expected conditions."""
    find_upload_by_id = MagicMock()
    find_upload_by_id.return_value = None  # upload record doesn't exist
    monkeypatch.setattr("cidc_api.models.ManifestUploads.find_by_id", find_upload_by_id)

    with pytest.raises(Exception, match="No manifest upload record found"):
        upload_postprocessing.derive_files_from_manifest_upload(event, None)

    # Mock existing upload record
    find_upload_by_id.return_value = MagicMock()

    # Ensure that file derivation happens so long as upload record exists
    _derive_files = MagicMock()
    monkeypatch.setattr(
        upload_postprocessing, "_derive_files_from_upload", _derive_files
    )

    upload_postprocessing.derive_files_from_manifest_upload(event, None)
    _derive_files.assert_called()


def test_assay_or_analysis_preconditions(monkeypatch):
    """Ensure derive_files_from_assay_or_analysis_upload blocks derivation under the expected conditions."""
    find_by_id = MagicMock()
    find_by_id.return_value = None
    monkeypatch.setattr(upload_postprocessing.AssayUploads, "find_by_id", find_by_id)

    with pytest.raises(Exception, match="No upload record with id"):
        upload_postprocessing.derive_files_from_assay_or_analysis_upload(event, None)

    find_by_id.return_value = AssayUploads(
        trial_id="foo", status=AssayUploadStatus.MERGE_FAILED.value
    )
    with pytest.raises(Exception, match="status is merge-failed"):
        upload_postprocessing.derive_files_from_assay_or_analysis_upload(event, None)

    find_by_id.return_value.status = AssayUploadStatus.MERGE_COMPLETED.value

    # Ensure that file derivation happens so long as upload record exists
    _derive_files = MagicMock()
    monkeypatch.setattr(
        upload_postprocessing, "_derive_files_from_upload", _derive_files
    )

    upload_postprocessing.derive_files_from_assay_or_analysis_upload(event, None)
    _derive_files.assert_called()


def test_derive_files_from_upload(monkeypatch):
    upload_to_data_bucket = MagicMock()
    upload_to_data_bucket.return_value = blob = MagicMock()
    monkeypatch.setattr(
        upload_postprocessing, "upload_to_data_bucket", upload_to_data_bucket
    )

    create_from_blob = MagicMock()
    monkeypatch.setattr(
        upload_postprocessing.DownloadableFiles, "create_from_blob", create_from_blob
    )

    derive_files = MagicMock()
    derive_files.return_value.artifacts = [
        upload_postprocessing.unprism.Artifact("", "", "", "", {})
    ]
    monkeypatch.setattr(upload_postprocessing.unprism, "derive_files", derive_files)

    session = MagicMock()

    # Call the function
    upload_postprocessing._derive_files_from_upload(
        "test-trial", "test-upload", session
    )

    # Check control flow
    derive_files.assert_called()
    upload_to_data_bucket.assert_called()
    create_from_blob.assert_called()
    session.commit.assert_called()
    assert blob in create_from_blob.call_args[0]
