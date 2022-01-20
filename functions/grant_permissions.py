import logging
import sys
from typing import Dict, List

from .settings import ENV, GOOGLE_WORKER_TOPIC
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session

from cidc_api.models import Permissions, Users
from cidc_api.shared.gcloud_client import (
    _encode_and_publish,
    get_blob_names,
    grant_download_access_to_blob_names,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


def grant_download_permissions(event: dict, context: BackgroundContext):
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict(eval(data))
    except:
        raise

    else:
        trial_id, upload_type = data.get("trial_id"), data.get("upload_type")
        if not trial_id or not upload_type:
            raise Exception(
                f"trial_id and upload_type must both be provided, you provided: {data}"
            )

        with sqlalchemy_session() as session:
            try:
                permissions_list: List[Permissions] = Permissions.get_for_trial_type(
                    trial_id=trial_id, upload_type=upload_type, session=session
                )
                user_list: List[Users] = [
                    Users.find_by_id(id=perm.granted_to_user, session=session)
                    for perm in permissions_list
                ]
                user_email_list: List[str] = [u.email for u in user_list]

                blob_list: List = get_blob_names(
                    trial_id=trial_id, upload_type=upload_type
                )

                n = 100  # number_of_blobs_per_chunk
                blob_list_chunks = [
                    blob_list[i : i + n] for i in range(0, len(blob_list), n)
                ]

                for chunk in blob_list_chunks:
                    kwargs = {
                        "_fn": "permissions_worker",
                        "user_list": user_email_list,
                        "blob_list": chunk,
                    }
                    report = _encode_and_publish(str(kwargs), GOOGLE_WORKER_TOPIC)
                    # Wait for response from pub/sub
                    if report:
                        report.result()

            except Exception as e:
                logger.error(repr(e))


def permissions_worker(user_list: List[str] = [], blob_list: List[str] = []):
    if not user_list or not blob_list:
        data = {"user_list": user_list, "blob_list": blob_list}
        raise Exception(
            f"Permissions worker: user_list and blob_list must both be provided, you provided: {data}"
        )

    try:
        # scvannost - I named user_email param suboptimally in the API; tech debt to fix
        grant_download_access_to_blob_names(user_email=user_list, blob_list=blob_list)
    except Exception as e:
        logger.error(repr(e))
