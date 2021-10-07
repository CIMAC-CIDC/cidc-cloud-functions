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
    manifest2 = {
        "protocol_identifier": "foo",
        "manifest_id": "baz",
        "samples": [],
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest, manifest2]
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
    assert mock_detect.call_count == 2
    for i in range(2):
        args, kwargs = mock_detect.call_args_list[i]
        assert (manifest, manifest2)[i] in args
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
        assert mock.call_count == 2
        for i in range(2):
            args, kwargs = mock.call_args_list[i]
            assert (manifest, manifest2)[i] in args
            assert kwargs.get("uploader_email") == UPLOADER_EMAIL
            assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and args[1].startswith(
        "Summary of Update from CSMS:"
    )
    assert (
        f"New {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in args[2]
    )
    assert (
        f"New {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in args[2]
    )

    # if throws any other error, does nothing but email
    reset()
    mock_detect.side_effect = Exception("foo")
    update_cidc_from_csms()
    mock_email.assert_called_once()
    args, _ = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and args[1].startswith(
        "Summary of Update from CSMS:"
    )
    assert (
        f"Problem with {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}: {Exception('foo')!s}"
        in args[2]
    )
    assert (
        f"Problem with {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')}: {Exception('foo')!s}"
        in args[2]
    )
    for mock in [
        mock_insert_json,
        mock_insert_blob,
    ]:
        mock.assert_not_called()
