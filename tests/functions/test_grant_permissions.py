import functions.grant_permissions
from functions.grant_permissions import grant_download_permissions, permissions_worker
from functions.settings import GOOGLE_WORKER_TOPIC
import pytest
from typing import Any, List, Optional, Set, Tuple, Union
from unittest.mock import MagicMock, call


def test_grant_download_permissions(monkeypatch):

    user_email_list = ["foo@bar.com", "user@test.com", "cidc@foo.bar"]
    full_email_dict = {
        None: {"bar": [user_email_list[0]]},
        "foo": {
            None: [user_email_list[1]],
            "bar": [user_email_list[2]],
            "baz": [user_email_list[2]],
        },
        "biz": {"wes": [user_email_list[2]]},
    }

    def mock_get_user_emails(
        trial_id: Optional[str], upload_type: Optional[Union[str, List[str]]], session
    ):
        def upload_matches(this_upload: Optional[str]):
            if this_upload is None:
                return True
            elif upload_type is None:
                return False
            if isinstance(upload_type, str):
                return this_upload == upload_type
            else:
                return this_upload in upload_type

        return {
            trial: {
                upload: users
                for upload, users in upload_dict.items()
                if upload_matches(upload)
            }
            for trial, upload_dict in full_email_dict.items()
            if trial is None or trial == trial_id
        }

    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "get_user_emails_for_trial_upload",
        mock_get_user_emails,
    )

    mock_blob_name_list = MagicMock()
    # need more than 100 to test chunking
    mock_blob_name_list.return_value = set([f"blob{n}" for n in range(100 + 50)])

    def mock_blob_list(
        trial_id: Optional[str], upload_type: Optional[Tuple[Optional[str]]], **kwargs
    ) -> Set[str]:
        """Type check and then pass through to the mock"""
        assert trial_id is None or isinstance(trial_id, str), type(trial_id)
        assert upload_type is None or (
            isinstance(upload_type, tuple)
            and all([isinstance(u, str) for u in upload_type])
        ), type(upload_type)
        return mock_blob_name_list(trial_id=trial_id, upload_type=upload_type, **kwargs)

    monkeypatch.setattr(functions.grant_permissions, "get_blob_names", mock_blob_list)

    mock_encode_and_publish = MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions, "_encode_and_publish", mock_encode_and_publish
    )

    # no matching does nothing at all, just logging
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = "{}"
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    with pytest.raises(
        Exception, match="trial_id and upload_type must both be provided, you provided:"
    ):
        grant_download_permissions({}, None)

    # incomplete/incorrect matching does nothing at all, just logging
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = str(
        {"trial_id": "foo", "user_email_list": ["baz"]}
    )
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    with pytest.raises(
        Exception, match="trial_id and upload_type must both be provided, you provided:"
    ):
        grant_download_permissions({}, None)

    # with data response, calls
    mock_extract_data = MagicMock()
    mock_extract_data.return_value = str({"trial_id": "foo", "upload_type": "bar"})
    monkeypatch.setattr(
        functions.grant_permissions, "extract_pubsub_data", mock_extract_data
    )
    grant_download_permissions({}, None)
    assert mock_blob_name_list.call_count == 3
    # (None, bar), (foo, None), (foo, bar) all match
    # Note no (biz, wes) as that doesn't match
    for _, kwargs in mock_blob_name_list.call_args_list:
        assert kwargs["trial_id"] in (None, "foo")
        assert kwargs["upload_type"] in (None, ("bar",))

    assert mock_encode_and_publish.call_count == 6
    assert mock_encode_and_publish.call_args_list == [
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[:1],
                    "blob_name_list": list(mock_blob_name_list.return_value)[:100],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[:1],
                    "blob_name_list": list(mock_blob_name_list.return_value)[100:],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[1:2],
                    "blob_name_list": list(mock_blob_name_list.return_value)[:100],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[1:2],
                    "blob_name_list": list(mock_blob_name_list.return_value)[100:],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[-1:],
                    "blob_name_list": list(mock_blob_name_list.return_value)[:100],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list[-1:],
                    "blob_name_list": list(mock_blob_name_list.return_value)[100:],
                    "revoke": False,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
    ]

    # with revoke: True, passing revoke: True
    # passing user_email_list doesn't get the Permissions or users
    mock_encode_and_publish.reset_mock()  # we're checking this
    mock_blob_name_list.reset_mock()

    def no_call(self):
        assert False

    monkeypatch.setattr(
        functions.grant_permissions.Permissions,
        "get_user_emails_for_trial_upload",
        lambda *args: no_call(),
    )

    mock_extract_data.return_value = str(
        {
            "trial_id": "foo",
            "upload_type": ["bar", "baz"],
            "user_email_list": user_email_list,
            "revoke": True,
        }
    )
    grant_download_permissions({}, None)

    assert mock_blob_name_list.call_count == 1
    _, kwargs = mock_blob_name_list.call_args
    assert kwargs["trial_id"] == "foo"
    assert kwargs["upload_type"] == ("bar", "baz")

    assert mock_encode_and_publish.call_count == 2
    assert mock_encode_and_publish.call_args_list == [
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list,
                    "blob_name_list": list(mock_blob_name_list.return_value)[:100],
                    "revoke": True,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
        call(
            str(
                {
                    "_fn": "permissions_worker",
                    "user_email_list": user_email_list,
                    "blob_name_list": list(mock_blob_name_list.return_value)[100:],
                    "revoke": True,
                }
            ),
            GOOGLE_WORKER_TOPIC,
        ),
    ]


def test_permissions_worker(monkeypatch):
    user_email_list = ["foo@bar.com", "user@test.com"]
    blob_name_list = [f"blob{n}" for n in range(100)]

    with pytest.raises(
        ValueError, match="user_email_list and blob_name_list must both be provided"
    ):
        permissions_worker()

    mock_grant, mock_revoke = MagicMock(), MagicMock()
    monkeypatch.setattr(
        functions.grant_permissions, "grant_download_access_to_blob_names", mock_grant
    )
    monkeypatch.setattr(
        functions.grant_permissions,
        "revoke_download_access_from_blob_names",
        mock_revoke,
    )
    permissions_worker(
        user_email_list=user_email_list,
        blob_name_list=blob_name_list,
        revoke=False,
    )
    mock_grant.assert_called_with(
        user_email_list=user_email_list,
        blob_name_list=blob_name_list,
    )
    mock_revoke.assert_not_called()

    mock_grant.reset_mock()
    permissions_worker(
        user_email_list=user_email_list,
        blob_name_list=blob_name_list,
        revoke=True,
    )
    mock_grant.assert_not_called()
    mock_revoke.assert_called_with(
        user_email_list=user_email_list,
        blob_name_list=blob_name_list,
    )
