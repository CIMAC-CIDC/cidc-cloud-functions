from datetime import datetime, timedelta

from sqlalchemy.orm import Query
from cidc_api.models import Users

from .util import sqlalchemy_session


def disable_inactive_users(*args):
    """Disable any users who haven't logged in for `INACTIVE_DAY_THRESHOLD` days."""
    with sqlalchemy_session() as session:
        print("Disabling inactive users...")
        Users.disable_inactive_users(session=True)
        print("done.")
