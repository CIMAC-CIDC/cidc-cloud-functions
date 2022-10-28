from datetime import datetime, timedelta
from typing import List

from cidc_api.models import Users
from cidc_api.shared.gcloud_client import (
    send_email,
    grant_bigquery_access,
    refresh_intake_access,
)
from cidc_api.shared.emails import CIDC_MAILING_LIST

from .settings import ENV
from .util import sqlalchemy_session


def disable_inactive_users(*args):
    """Disable any users who have become inactive."""
    with sqlalchemy_session() as session:
        print("Disabling inactive users...")
        disabled: List[str] = Users.disable_inactive_users(session=session)
        print(f"Disabled inactive on {ENV}: {disabled}")
        if len(disabled):
            send_email(
                CIDC_MAILING_LIST,
                f"({ENV}) Disabled inactivate user(s): {datetime.now()}",
                f"Disabled following inactive user(s) on {ENV}s: {disabled}",
            )


def refresh_download_permissions(*args):
    """
    Extend the expiry date for GCS download permissions belonging to users
    who accessed the system in the last 2 (or so) days. If we don't do this,
    users whose accounts are still active might lose GCS download permission prematurely.
    """
    active_today = lambda q: q.filter(
        # Provide a 3 day window to ensure we don't miss anyone
        # if, e.g., this function fails to run on a certain day.
        Users._accessed
        > datetime.today() - timedelta(days=3)
    )
    with sqlalchemy_session() as session:
        active_users = Users.list(
            page_size=Users.count(session=session, filter_=active_today),
            session=session,
            filter_=active_today,
        )
        for user in active_users:
            print(f"Refreshing IAM upload permissions for {user.email}")
            refresh_intake_access(user.email)
        grant_bigquery_access([user.email for user in active_users])
