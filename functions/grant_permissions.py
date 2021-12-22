import logging
import sys

from .settings import ENV
from .util import BackgroundContext, sqlalchemy_session

from cidc_api.models import Permissions

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


def grant_all_download_permissions(event: dict, context: BackgroundContext):
    with sqlalchemy_session() as session:
        try:
            Permissions.grant_all_download_permissions(session=session)
        except Exception as e:
            logger.error(str(e))
