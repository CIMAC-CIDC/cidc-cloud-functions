from unittest.mock import MagicMock

from tests.util import with_app_context

import functions.csms
from functions.csms import UPLOADER_EMAIL, update_cidc_from_csms

from cidc_api.models.templates.csms_api import Change, NewManifestError
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
    mock_insert_records, mock_email = MagicMock(), MagicMock()
    monkeypatch.setattr(functions.csms, "insert_record_batch", mock_insert_records)
    monkeypatch.setattr(functions.csms, "send_email", mock_email)

    mock_detect = MagicMock()
    monkeypatch.setattr(functions.csms, "detect_manifest_changes", mock_detect)
    mock_update = MagicMock()
    monkeypatch.setattr(functions.csms, "update_with_changes", mock_update)

    def reset():
        for mock in [
            mock_api_get,
            mock_insert_json,
            mock_insert_blob,
            mock_insert_records,
            mock_email,
            mock_update,
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
        mock_insert_records,
        mock_email,
        mock_update,
    ]:
        mock.assert_not_called()

    # if changes, pass first return to insert_record_batch and second to update_with_changes
    reset()
    changes = [Change("sample", "foo", "bar", "baz", {"change": ("old", "new")})]
    mock_detect.return_value = ({"foo": ["bar", "baz"]}, changes)  # records, changes
    update_cidc_from_csms()
    mock_insert_records.assert_called_once()
    args, kwargs = mock_insert_records.call_args_list[0]
    assert {"foo": ["bar", "baz"]} in args
    assert "session" in kwargs

    mock_update.assert_called_once()
    args, kwargs = mock_update.call_args_list[0]
    assert changes in args
    assert "session" in kwargs

    mock_email.assert_called_once_with(
        CIDC_MAILING_LIST, f"Changes for foo manifest bar", str(changes),
    )
    for mock in [mock_insert_json, mock_insert_blob]:
        mock.assert_not_called()
    mock_email.reset_mock()

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
