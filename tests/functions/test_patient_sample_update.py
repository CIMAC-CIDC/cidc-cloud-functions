from unittest.mock import MagicMock

import pytest

from functions import patient_sample_update

from tests.util import make_pubsub_event


def test_generate_csvs(monkeypatch):
    """Ensure generate_csvs follows the expected control flow."""
    trial_id = "test-trial"
    event = make_pubsub_event(trial_id)

    find_upload_by_id = MagicMock()
    find_upload_by_id.return_value = None
    monkeypatch.setattr("cidc_api.models.ManifestUploads.find_by_id", find_upload_by_id)

    with pytest.raises(Exception, match="No manifest upload record found"):
        patient_sample_update.generate_csvs(event, None)

    find_upload_by_id.return_value = MagicMock()

    find_trial_by_id = MagicMock()
    monkeypatch.setattr("cidc_api.models.TrialMetadata.find_by_id", find_trial_by_id)

    _upload_to_data_bucket = MagicMock()
    _upload_to_data_bucket.return_value = blob = MagicMock()
    monkeypatch.setattr(
        patient_sample_update, "upload_to_data_bucket", _upload_to_data_bucket
    )

    create_from_blob = MagicMock()
    monkeypatch.setattr(
        patient_sample_update.DownloadableFiles, "create_from_blob", create_from_blob
    )

    derive_files = MagicMock()
    derive_files.return_value.artifacts = [
        patient_sample_update.unprism.Artifact("", "", "", "", {})
    ]
    monkeypatch.setattr(patient_sample_update.unprism, "derive_files", derive_files)

    # Call the function
    patient_sample_update.generate_csvs(event, None)

    # Check control flow
    find_upload_by_id.assert_called()
    find_trial_by_id.assert_called()
    derive_files.assert_called()
    _upload_to_data_bucket.assert_called()
    create_from_blob.assert_called()
    assert blob in create_from_blob.call_args[0]
