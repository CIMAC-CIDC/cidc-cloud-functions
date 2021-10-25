import pytest
from unittest.mock import MagicMock

from tests.util import make_pubsub_event, with_app_context

import functions.csms
from functions.csms import UPLOADER_EMAIL, update_cidc_from_csms

from cidc_api.models.templates.csms_api import NewManifestError
from cidc_api.shared.emails import CIDC_MAILING_LIST


@with_app_context
def test_update_cidc_from_csms_matching_some(monkeypatch):
    manifest = {
        "protocol_identifier": "foo",
        "manifest_id": "bar",
        "samples": [{}],  # len != 0
    }
    manifest2 = {
        "protocol_identifier": "foobar",
        "manifest_id": "baz",
        "samples": [{}],  # len != 0
    }
    manifest3 = {
        "protocol_identifier": "foo",
        "manifest_id": "biz",
        "samples": [{}],  # len != 0
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest, manifest2, manifest3]
    mock_extract_info_from_manifest = lambda m, session: (
        m["protocol_identifier"],
        m["manifest_id"],
        [],
    )
    monkeypatch.setattr(functions.csms, "get_with_paging", mock_api_get)
    monkeypatch.setattr(
        functions.csms, "_extract_info_from_manifest", mock_extract_info_from_manifest
    )

    mock_insert_json, mock_insert_blob = MagicMock(), MagicMock()
    monkeypatch.setattr(functions.csms, "insert_manifest_from_json", mock_insert_json)
    monkeypatch.setattr(functions.csms, "insert_manifest_into_blob", mock_insert_blob)
    mock_email = MagicMock()
    monkeypatch.setattr(functions.csms, "send_email", mock_email)

    mock_detect = MagicMock()
    mock_detect.side_effect = NewManifestError()
    monkeypatch.setattr(functions.csms, "detect_manifest_changes", mock_detect)

    mock_logger = MagicMock()
    monkeypatch.setattr(functions.csms, "logger", mock_logger)

    def reset():
        for mock in [
            mock_api_get,
            mock_insert_json,
            mock_insert_blob,
            mock_email,
            mock_logger,
        ]:
            mock.reset_mock()

    # if matches on the trial_id, only changes those
    mock_detect.return_value = ({}, [])  # records, changes
    mock_api_get.return_value = [manifest, manifest3]
    match_trial_event = make_pubsub_event(str({"trial_id": "foo", "manifest_id": "*"}))
    update_cidc_from_csms(match_trial_event, None)
    assert all(
        [
            "trial_id=foo" in args[0] and "manifest_id" not in args[0]
            for args, _ in mock_api_get.call_args_list
        ]
    )
    for mock in [mock_insert_blob, mock_insert_json]:
        assert mock.call_count == 2
        for i in range(2):
            args, kwargs = mock.call_args_list[i]
            assert (manifest, manifest3)[i] in args
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
        f"New {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')}"
        not in args[2]
    )
    assert (
        f"New {manifest3.get('protocol_identifier')} manifest {manifest3.get('manifest_id')} with {len(manifest3.get('samples', []))} samples"
        in args[2]
    )

    reset()
    # if matches on the manifest_id, only changes that one
    # manifest_id is asserted to be unique in the CIDC database
    mock_api_get.return_value = [manifest2]
    match_trial_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "baz"}))
    update_cidc_from_csms(match_trial_event, None)
    assert all(
        [
            "manifest_id=baz" in args[0] and "trial_id" not in args[0]
            for args, _ in mock_api_get.call_args_list
        ]
    )
    for mock in [mock_insert_blob, mock_insert_json]:
        assert mock.call_count == 1
        args, kwargs = mock.call_args_list[0]
        assert manifest2 in args
        assert kwargs.get("uploader_email") == UPLOADER_EMAIL
        assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and args[1].startswith(
        "Summary of Update from CSMS:"
    )
    assert (
        f"New {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}"
        not in args[2]
    )
    assert (
        f"New {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in args[2]
    )
    assert (
        f"New {manifest3.get('protocol_identifier')} manifest {manifest3.get('manifest_id')}"
        not in args[2]
    )

    reset()
    # if matches on the trial_id and manifest_id, only changes that one
    mock_detect.return_value = ({}, [])  # records, changes
    mock_api_get.return_value = [manifest]
    match_trial_event = make_pubsub_event(
        str({"trial_id": "foo", "manifest_id": "bar"})
    )
    update_cidc_from_csms(match_trial_event, None)
    assert all(
        [
            "trial_id=foo" in args[0] and "manifest_id=bar" in args[0]
            for args, _ in mock_api_get.call_args_list
        ]
    )
    for mock in [mock_insert_blob, mock_insert_json]:
        assert mock.call_count == 1
        args, kwargs = mock.call_args_list[0]
        assert manifest in args
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
        f"New {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')}"
        not in args[2]
    )
    assert (
        f"New {manifest3.get('protocol_identifier')} manifest {manifest3.get('manifest_id')}"
        not in args[2]
    )

    reset()
    # if matches none, does nothing
    mock_detect.return_value = ({}, [])  # records, changes
    mock_api_get.return_value = []
    match_trial_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "foo"}))
    assert all(
        [
            "manifest_id=foo" in args[0] and "trial_id" not in args[0]
            for args, _ in mock_api_get.call_args_list
        ]
    )
    update_cidc_from_csms(match_trial_event, None)
    for mock in [mock_insert_blob, mock_insert_json, mock_email]:
        assert mock.call_count == 0

    reset()
    # if throws error, doesn't call insert functions at all even tho they're new
    # empty dict throws KeyError on event["data"]
    mock_api_get.return_value = [manifest, manifest2, manifest3]
    update_cidc_from_csms({}, None)
    assert all(
        [
            "trial_id" not in args[0] and "manifest_id" not in args[0]
            for args, _ in mock_api_get.call_args_list
        ]
    )
    for mock in [mock_insert_blob, mock_insert_json]:
        assert mock.call_count == 0
    mock_logger.warning.assert_called_once()
    args, _ = mock_logger.warning.call_args_list[0]
    assert (
        "Both trial_id and manifest_id matching must be provided, no actual data changes will be made."
        in args[0]
    )

    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and args[1].startswith(
        "Summary of Update from CSMS:"
    )
    assert (
        "Both trial_id and manifest_id matching must be provided, no actual data changes will be made."
        in args[2]
    )
    assert (
        f"Would add new {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in args[2]
    )
    assert (
        f"Would add new {manifest2.get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in args[2]
    )
    assert (
        f"Would add new {manifest3.get('protocol_identifier')} manifest {manifest3.get('manifest_id')} with {len(manifest3.get('samples', []))} samples"
        in args[2]
    )

    # if bad-key but correctly formatted event data, error directly
    mock_detect.side_effect = Exception("foo")
    bad_event = make_pubsub_event(str({"key": "value"}))
    with pytest.raises(
        Exception, match="Both trial_id and manifest_id matching must be provided"
    ):
        update_cidc_from_csms(bad_event, None)


@with_app_context
def test_update_cidc_from_csms_matching_all(monkeypatch):
    manifest = {
        "protocol_identifier": "foo",
        "manifest_id": "bar",
        "samples": [{}],  # len != 0
    }
    manifest2 = {
        "protocol_identifier": "foo",
        "manifest_id": "baz",
        "samples": [{}],  # len != 0
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest, manifest2]
    mock_extract_info_from_manifest = lambda m, session: (
        m["protocol_identifier"],
        m["manifest_id"],
        [],
    )
    monkeypatch.setattr(functions.csms, "get_with_paging", mock_api_get)
    monkeypatch.setattr(
        functions.csms, "_extract_info_from_manifest", mock_extract_info_from_manifest
    )

    mock_insert_json, mock_insert_blob = MagicMock(), MagicMock()
    monkeypatch.setattr(functions.csms, "insert_manifest_from_json", mock_insert_json)
    monkeypatch.setattr(functions.csms, "insert_manifest_into_blob", mock_insert_blob)
    mock_email = MagicMock()
    monkeypatch.setattr(functions.csms, "send_email", mock_email)

    mock_detect = MagicMock()
    monkeypatch.setattr(functions.csms, "detect_manifest_changes", mock_detect)

    match_all_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "*"}))

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
    update_cidc_from_csms(match_all_event, None)
    assert all(["*" not in args[0] for args, _ in mock_api_get.call_args_list])
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
    update_cidc_from_csms(match_all_event, None)
    assert all("*" not in args for args, _ in mock_api_get.call_args_list)
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
    update_cidc_from_csms(match_all_event, None)
    assert all("*" not in args for args, _ in mock_api_get.call_args_list)
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
