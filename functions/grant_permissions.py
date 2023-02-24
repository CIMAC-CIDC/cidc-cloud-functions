from datetime import datetime
import logging
import sys
from typing import Dict, List, Optional, Tuple, Union

from .settings import ENV, GOOGLE_WORKER_TOPIC
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session

from cidc_api.models import Permissions
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


BLOBS_PER_CHUNK: int = 100  # how many files to bundle together for permissions handling


def grant_download_permissions(event: dict, context: BackgroundContext):
    """
    Takes the pubsub event data as dict

    Parameters
    ----------
    trial_id: Optional[str]
        the trial_id for the trial to affect
        explicitly pass None for cross-trial
    upload_type: Optional[Union[str, List[str]]]
        the upload_type, as stored in the Permissions table
        explicitly pass None for cross-assay (excludes clinical_data)

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
        raw_data: str = extract_pubsub_data(event)
        data: dict = dict(eval(raw_data))
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

        revoke = data.get("revoke", False)
        trial_id: Optional[str] = data.get("trial_id")

        upload_type: Optional[Tuple[str]] = None  # this None will always be replaced
        raw_upload_type: Optional[Union[str, List[str]]] = data.get("upload_type")
        if raw_upload_type:
            if isinstance(raw_upload_type, str):
                upload_type = (raw_upload_type,)
            else:
                upload_type = tuple(raw_upload_type)
        else:
            upload_dict = raw_upload_type  # type: ignore

        with sqlalchemy_session() as session:
            user_email_dict: Dict[
                Optional[str], Dict[Optional[Tuple[str]], List[str]]
            ] = {}  # this empty dict will always be replaced
            try:
                if data.get("user_email_list"):
                    user_email_dict = {trial_id: {upload_type: data["user_email_list"]}}

                else:
                    user_email_dict = Permissions.get_user_emails_for_trial_upload(
                        trial_id=data.get("trial_id"),
                        upload_type=data.get("upload_type"),
                        session=session,
                    )

                blob_name_dict: Dict[
                    Optional[str], Dict[Optional[Tuple[str]], List[str]]
                ] = {
                    trial: {
                        upload: list(
                            get_blob_names(
                                trial_id=trial, upload_type=upload, session=session
                            )
                        )
                        for upload in upload_dict.keys()
                    }
                    for trial, upload_dict in user_email_dict.items()
                }

                upload_dict: Dict[Optional[Tuple[str]], List[str]]
                for trial_id, upload_dict in blob_name_dict.items():
                    upload: Optional[Tuple[str]]
                    blob_name_list: List[str]
                    for upload, blob_name_list in upload_dict.items():
                        user_email_list: List[str] = user_email_dict.get(
                            trial_id, {}
                        ).get(upload, [])

                        if not user_email_list or not blob_name_list:
                            continue

                        blob_name_list_chunks = [
                            blob_name_list[i : i + BLOBS_PER_CHUNK]
                            for i in range(0, len(blob_name_list), BLOBS_PER_CHUNK)
                        ]

                        for chunk in blob_name_list_chunks:
                            kwargs = {
                                "_fn": "permissions_worker",
                                "user_email_list": user_email_list,
                                "blob_name_list": chunk,
                                "revoke": revoke,
                            }

                            report = _encode_and_publish(
                                str(kwargs), GOOGLE_WORKER_TOPIC
                            )
                            # Wait for response from pub/sub
                            if report:
                                report.result()

            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                send_email(
                    CIDC_MAILING_LIST,
                    f"[DEV ALERT]({ENV})Error granting permissions: {datetime.now()}",
                    html_content=f"See logs for more info<br />{e}<br />For {data}",
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
                user_email_list=user_email_list,
                blob_name_list=blob_name_list,
            )
        else:
            grant_download_access_to_blob_names(
                user_email_list=user_email_list,
                blob_name_list=blob_name_list,
            )
    except Exception as e:
        data = {"user_email_list": user_email_list, "blob_name_list": blob_name_list}
        logger.error(f"Error on {data}:\nError:{e}", exc_info=True)
        # send_email(
        #     CIDC_MAILING_LIST,
        #     f"[DEV ALERT]({ENV}) Error granting permissions: {datetime.now()}",
        #     html_content=f"See logs for more info.<br />{e}<br /> For: {data}",
        # )
        raise e
