import logging
import sys

from .settings import ENV
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session

from cidc_api.models import Permissions

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


def grant_all_download_permissions(event: dict, context: BackgroundContext):
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict(eval(data))
    except:
        raise

    else:
        trial_id = data.get("trial_id")
        if not trial_id:
            raise Exception(f"trial_id must both be provided, you provided: {data}")

        with sqlalchemy_session() as session:
            try:
                Permissions.grant_all_download_permissions(
                    trial_id=trial_id, session=session
                )
            except Exception as e:
                logger.error(repr(e))
