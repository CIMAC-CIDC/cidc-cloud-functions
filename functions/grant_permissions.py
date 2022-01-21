from datetime import datetime
import logging
import sys
from typing import List

from .settings import ENV, GOOGLE_WORKER_TOPIC
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session

from cidc_api.models import Permissions, Users
from cidc_api.shared.gcloud_client import (
    _encode_and_publish,
    get_blob_names,
    grant_download_access_to_blob_names,
    send_email,
    revoke_download_access_from_blob_names,
)
from cidc_api.shared.emails import CIDC_MAILING_LIST


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


def grant_download_permissions(event: dict, context: BackgroundContext):
    """
    Takes the pubsub event data as dict

    Parameters
    ----------
    trial_id: Optional[str]
        the trial_id for the trial to affect
    upload_type: Optional[str]
        the upload_type, as stored in the Permissions table

    Optional Parameters
    -------------------
    revoke: Optional[bool] = False
        if explicitly set to True, revoke instead of grant
    user_email_list: List[str]
        a comma separated list of user emails to apply the permissions for
        otherwise loaded from the database for all affected users
    """
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict(eval(data))
    except:
        raise

    else:
        # ---- Run API through here ----
        # handle special values for cross-assay/trial
        # API functions and database use None to mean all
        # but require explicit passing of None to match
        if "trial_id" not in data or "upload_type" not in data:
            raise Exception(
                f"trial_id and upload_type must both be provided, you provided: {data}\nProvide None for cross-trial/assay matching"
            )
        trial_id, upload_type = data.get("trial_id"), data.get("upload_type")

        revoke = data.get("revoke", False)

        with sqlalchemy_session() as session:
            try:
                if "user_email_list" in data:
                    user_email_list: List[str] = data["user_email_list"]

                else:
                    permissions_list: List[
                        Permissions
                    ] = Permissions.get_for_trial_type(
                        trial_id=trial_id, upload_type=upload_type, session=session
                    )
                    user_list: List[Users] = [
                        Users.find_by_id(id=perm.granted_to_user, session=session)
                        for perm in permissions_list
                    ]
                    user_email_list: List[str] = [u.email for u in user_list]

                blob_name_list: List = get_blob_names(
                    trial_id=trial_id, upload_type=upload_type
                )

                n = 100  # number_of_blobs_per_chunk
                blob_name_list_chunks = [
                    blob_name_list[i : i + n] for i in range(0, len(blob_name_list), n)
                ]

                for chunk in blob_name_list_chunks:
                    kwargs = {
                        "_fn": "permissions_worker",
                        "user_email_list": user_email_list,
                        "blob_name_list": chunk,
                        "revoke": revoke,
                    }
                    report = _encode_and_publish(str(kwargs), GOOGLE_WORKER_TOPIC)
                    # Wait for response from pub/sub
                    if report:
                        report.result()

            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                send_email(
                    CIDC_MAILING_LIST,
                    f"Error granting permissions: {datetime.now()}",
                    f"For {data}\r\nSee logs for more info\r\n{e}",
                )
                raise e


def permissions_worker(
    user_email_list: List[str] = [],
    blob_name_list: List[str] = [],
    revoke: bool = False,
):
    if not user_email_list or not blob_name_list:
        data = {"user_email_list": user_email_list, "blob_name_list": blob_name_list}
        raise ValueError(
            f"Permissions worker: user_email_list and blob_name_list must both be provided, you provided: {data}"
        )

    try:
        if revoke:
            revoke_download_access_from_blob_names(
                user_email_list=user_email_list, blob_name_list=blob_name_list
            )
        else:
            grant_download_access_to_blob_names(
                user_email_list=user_email_list, blob_name_list=blob_name_list
            )
    except Exception as e:
        data = {"user_email_list": user_email_list, "blob_name_list": blob_name_list}
        logger.error(f"Error on {data}:\nError:{e}", exc_info=True)
        send_email(
            CIDC_MAILING_LIST,
            f"Error granting permissions: {datetime.now()}",
            f"For {data}\r\nSee logs for more info\r\n{e}",
        )
        raise e
