from unittest.mock import MagicMock

from tests.util import with_app_context

import functions.csms
from functions.csms import UPLOADER_EMAIL, update_cidc_from_csms

from cidc_api.models.templates.csms_api import NewManifestError
from cidc_api.shared.emails import CIDC_MAILING_LIST


@with_app_context
def test_update_cidc_from_csms(monkeypatch):
    manifest = {
        "protocol_identifier": "foo",
        "manifest_id": "bar",
        "samples": [],
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest]
    monkeypatch.setattr(functions.csms, "get_with_paging", mock_api_get)

    mock_insert_json, mock_insert_blob = MagicMock(), MagicMock()
    monkeypatch.setattr(functions.csms, "insert_manifest_from_json", mock_insert_json)
    monkeypatch.setattr(functions.csms, "insert_manifest_into_blob", mock_insert_blob)
    mock_email = MagicMock()
    monkeypatch.setattr(functions.csms, "send_email", mock_email)

    mock_detect = MagicMock()
    monkeypatch.setattr(functions.csms, "detect_manifest_changes", mock_detect)

    def reset():
        for mock in [
            mock_api_get,
            mock_insert_json,
            mock_insert_blob,
            mock_email,
        ]:
            mock.reset_mock()

    # if no changes, nothing happens
    mock_detect.return_value = ({}, [])  # records, changes
    update_cidc_from_csms()
    mock_detect.assert_called_once()
    args, kwargs = mock_detect.call_args_list[0]
    assert manifest in args
    assert kwargs.get("uploader_email") == UPLOADER_EMAIL
    assert "session" in kwargs

    for mock in [
        mock_insert_json,
        mock_insert_blob,
        mock_email,
    ]:
        mock.assert_not_called()

    # if throws NewManifestError, calls insert functions with manifest itself
    reset()
    mock_detect.side_effect = NewManifestError()
    update_cidc_from_csms()
    for mock in [mock_insert_blob, mock_insert_json]:
        mock.assert_called_once()
        args, kwargs = mock.call_args_list[0]
        assert manifest in args
        assert kwargs.get("uploader_email") == UPLOADER_EMAIL
        assert "session" in kwargs
    mock_email.assert_called_once_with(
        CIDC_MAILING_LIST,
        f"Changes for {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}",
        f"New manifest with {len(manifest.get('samples', []))} samples",
    )

    # if throws any other error, does nothing but email
    reset()
    mock_detect.side_effect = Exception("foo")
    update_cidc_from_csms()
    mock_email.assert_called_once_with(
        CIDC_MAILING_LIST,
        f"Problem with {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}",
        "foo",
    )

    for mock in [
        mock_insert_json,
        mock_insert_blob,
    ]:
        mock.assert_not_called()
