import pytest
from unittest.mock import MagicMock

from tests.util import make_pubsub_event, with_app_context

import functions.csms

# mock the env setting
functions.csms.INTERNAL_USER_EMAIL = "user@email.com"

from functions.csms import INTERNAL_USER_EMAIL, update_cidc_from_csms

from cidc_api.models.csms_api import NewManifestError
from cidc_api.shared.emails import CIDC_MAILING_LIST


@with_app_context
def test_update_cidc_from_csms_matching_some(monkeypatch):
    manifest = {
        "manifest_id": "bar",
        "samples": [{"protocol_identifier": "foo"}],  # len != 0
    }
    manifest2 = {
        "manifest_id": "baz",
        "samples": [{"protocol_identifier": "foobar"}],  # len != 0
    }
    manifest3 = {
        "manifest_id": "biz",
        "samples": [{"protocol_identifier": "foo"}],  # len != 0
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest, manifest2, manifest3]
    monkeypatch.setattr(functions.csms, "get_with_paging", mock_api_get)

    mock_insert_blob = MagicMock()
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
        ["manifest_id" not in args[0] for args, _ in mock_api_get.call_args_list]
    )
    assert mock_insert_blob.call_count == 2
    for i in range(2):
        args, kwargs = mock_insert_blob.call_args_list[i]
        assert (manifest, manifest3)[i] in args
        assert kwargs.get("uploader_email") == INTERNAL_USER_EMAIL
        assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        f"New {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"New {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')}"
        not in kwargs["html_content"]
    )
    assert (
        f"New {manifest3.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest3.get('manifest_id')} with {len(manifest3.get('samples', []))} samples"
        in kwargs["html_content"]
    )

    reset()
    # if matches on the manifest_id, only changes that one
    # manifest_id is asserted to be unique in the CIDC database
    mock_api_get.return_value = [manifest2]
    match_trial_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "baz"}))
    update_cidc_from_csms(match_trial_event, None)
    assert all(
        ["manifest_id=baz" in args[0] for args, _ in mock_api_get.call_args_list]
    )
    assert mock_insert_blob.call_count == 1
    args, kwargs = mock_insert_blob.call_args_list[0]
    assert manifest2 in args
    assert kwargs.get("uploader_email") == INTERNAL_USER_EMAIL
    assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        f"New {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')}"
        not in kwargs["html_content"]
    )
    assert (
        f"New {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"New {manifest3.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest3.get('manifest_id')}"
        not in kwargs["html_content"]
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
        ["manifest_id=bar" in args[0] for args, _ in mock_api_get.call_args_list]
    )
    assert mock_insert_blob.call_count == 1
    args, kwargs = mock_insert_blob.call_args_list[0]
    assert manifest in args
    assert kwargs.get("uploader_email") == INTERNAL_USER_EMAIL
    assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        f"New {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"New {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')}"
        not in kwargs["html_content"]
    )
    assert (
        f"New {manifest3.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest3.get('manifest_id')}"
        not in kwargs["html_content"]
    )

    reset()
    # if matches none, does nothing
    mock_detect.return_value = ({}, [])  # records, changes
    mock_api_get.return_value = []
    match_trial_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "foo"}))
    assert all(
        ["manifest_id=foo" in args[0] for args, _ in mock_api_get.call_args_list]
    )
    update_cidc_from_csms(match_trial_event, None)
    for mock in [mock_insert_blob, mock_email]:
        assert mock.call_count == 0

    reset()
    # if throws error, doesn't call insert functions at all even tho they're new
    # empty dict throws KeyError on event["data"]
    mock_api_get.return_value = [manifest, manifest2, manifest3]
    update_cidc_from_csms({}, None)
    assert all(
        ["manifest_id" not in args[0] for args, _ in mock_api_get.call_args_list]
    )
    assert mock_insert_blob.call_count == 0
    mock_logger.info.assert_called()
    args, _ = mock_logger.info.call_args_list[0]
    assert "Dry-run call to update CIDC from CSMS." in args[0]

    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        "To make changes, trial_id and manifest_id matching must both be provided in the event data."
        in kwargs["html_content"]
    )
    assert (
        f"Would add new {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"Would add new {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"Would add new {manifest3.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest3.get('manifest_id')} with {len(manifest3.get('samples', []))} samples"
        in kwargs["html_content"]
    )

    # if bad-key but correctly formatted event data, error directly
    mock_detect.side_effect = Exception("foo")
    bad_event = make_pubsub_event(str({"key": "value"}))
    with pytest.raises(
        Exception, match="trial_id and manifest_id matching must both be provided"
    ):
        update_cidc_from_csms(bad_event, None)
    mock_detect.side_effect = None
    mock_detect.side_effect = Exception("foo")
    bad_event = make_pubsub_event(str({"manifest_id": "value"}))
    with pytest.raises(
        Exception, match="trial_id and manifest_id matching must both be provided"
    ):
        update_cidc_from_csms(bad_event, None)
    mock_detect.side_effect = None
    mock_detect.side_effect = Exception("foo")
    bad_event = make_pubsub_event(str({"trial_id": "value"}))
    with pytest.raises(
        Exception, match="trial_id and manifest_id matching must both be provided"
    ):
        update_cidc_from_csms(bad_event, None)
    mock_detect.side_effect = None

    reset()
    mock_detect.side_effect = NewManifestError()
    mock_insert_blob.side_effect = Exception("Error from insert_manifest_into_blob")
    match_all_event = make_pubsub_event(str({"manifest_id": "*", "trial_id": "*"}))
    update_cidc_from_csms(match_all_event, None)

    for n, mock in enumerate(
        [
            mock_api_get,
            mock_insert_blob,
            mock_email,
            mock_logger.error,
        ]
    ):
        assert mock.call_count >= 1, [
            "api",
            "insert_blob",
            "email",
            "logger",
        ][n]
    args, kwargs = mock_email.call_args_list[0]
    assert (
        f"Problem with {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')}"
        in kwargs["html_content"]
    )
    assert "Error from insert_manifest_into_blob" in kwargs["html_content"]


@with_app_context
def test_update_cidc_from_csms_matching_all(monkeypatch):
    manifest = {
        "manifest_id": "bar",
        "samples": [{"protocol_identifier": "foo"}],  # len != 0
    }
    manifest2 = {
        "manifest_id": "baz",
        "samples": [{"protocol_identifier": "foo"}],  # len != 0
    }

    mock_api_get = MagicMock()
    mock_api_get.return_value = [manifest, manifest2]
    monkeypatch.setattr(functions.csms, "get_with_paging", mock_api_get)

    mock_insert_blob = MagicMock()
    monkeypatch.setattr(functions.csms, "insert_manifest_into_blob", mock_insert_blob)
    mock_email = MagicMock()
    monkeypatch.setattr(functions.csms, "send_email", mock_email)

    mock_detect = MagicMock()
    monkeypatch.setattr(functions.csms, "detect_manifest_changes", mock_detect)

    match_all_event = make_pubsub_event(str({"trial_id": "*", "manifest_id": "*"}))

    def reset():
        for mock in [mock_api_get, mock_insert_blob, mock_email]:
            mock.reset_mock()

    # if no changes, nothing happens
    mock_detect.return_value = ({}, [])  # records, changes
    update_cidc_from_csms(match_all_event, None)
    assert all(["*" not in args[0] for args, _ in mock_api_get.call_args_list])
    assert mock_detect.call_count == 2
    for i in range(2):
        args, kwargs = mock_detect.call_args_list[i]
        assert (manifest, manifest2)[i] in args
        print(kwargs)
        assert kwargs.get("uploader_email") == INTERNAL_USER_EMAIL
        assert "session" in kwargs

    for mock in [mock_insert_blob, mock_email]:
        mock.assert_not_called()

    # if throws NewManifestError, calls insert functions with manifest itself
    reset()
    mock_detect.side_effect = NewManifestError()
    update_cidc_from_csms(match_all_event, None)
    assert all("*" not in args for args, _ in mock_api_get.call_args_list)
    assert mock_insert_blob.call_count == 2
    for i in range(2):
        args, kwargs = mock_insert_blob.call_args_list[i]
        assert (manifest, manifest2)[i] in args
        assert kwargs.get("uploader_email") == INTERNAL_USER_EMAIL
        assert "session" in kwargs
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        f"New {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
        in kwargs["html_content"]
    )
    assert (
        f"New {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')} with {len(manifest2.get('samples', []))} samples"
        in kwargs["html_content"]
    )

    # if throws any other error, does nothing but email
    reset()
    mock_detect.side_effect = Exception("foo")
    update_cidc_from_csms(match_all_event, None)
    assert all("*" not in args for args, _ in mock_api_get.call_args_list)
    mock_email.assert_called_once()
    args, kwargs = mock_email.call_args_list[0]
    assert args[0] == CIDC_MAILING_LIST and "CSMS" in args[1]
    assert (
        f"Problem with {manifest.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest.get('manifest_id')}: {Exception('foo')!r}"
        in kwargs["html_content"]
    )
    assert (
        f"Problem with {manifest2.get('samples', [{}])[0].get('protocol_identifier')} manifest {manifest2.get('manifest_id')}: {Exception('foo')!r}"
        in kwargs["html_content"]
    )
    mock_insert_blob.assert_not_called()
