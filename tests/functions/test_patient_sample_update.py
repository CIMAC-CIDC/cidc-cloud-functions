from unittest.mock import MagicMock

from functions.patient_sample_update import make_patient_sample_csvs

from tests.util import make_pubsub_event


def test_make_patient_sample_csvs(monkeypatch):
    """Ensure make_patient_sample_csvs follows the expected control flow."""

    generate_patient_csv = MagicMock()
    generate_patient_csv.return_value = "patient csv"
    generate_sample_csv = MagicMock()
    generate_sample_csv.return_value = "samples csv"

    monkeypatch.setattr(
        "cidc_api.models.TrialMetadata.generate_patient_csv", generate_patient_csv
    )
    monkeypatch.setattr(
        "cidc_api.models.TrialMetadata.generate_sample_csv", generate_sample_csv
    )

    _upload_to_data_bucket = MagicMock()
    _upload_to_data_bucket.return_value = blob = MagicMock()
    monkeypatch.setattr(
        "functions.patient_sample_update._upload_to_data_bucket", _upload_to_data_bucket
    )

    create_from_blob = MagicMock()
    monkeypatch.setattr(
        "cidc_api.models.DownloadableFiles.create_from_blob", create_from_blob
    )

    trial_id = "test-trial"

    # Call the function
    make_patient_sample_csvs(make_pubsub_event(trial_id), None)

    # Check control flow
    assert generate_patient_csv.call_args[0][0] == trial_id
    assert generate_sample_csv.call_args[0][0] == trial_id
    _upload_to_data_bucket.assert_called()
    create_from_blob.assert_called()
    assert blob in create_from_blob.call_args[0]
