from datetime import datetime, timedelta

from sqlalchemy.orm import Query
from cidc_api.models import Users

from .util import sqlalchemy_session


def disable_inactive_users(*args):
    """Disable any users who have become inactive."""
    with sqlalchemy_session() as session:
        print("Disabling inactive users...")
        Users.disable_inactive_users(session=True)
        print("done.")
