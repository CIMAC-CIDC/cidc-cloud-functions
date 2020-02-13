import json
import time
import requests
from datetime import datetime
from typing import List, Optional

from google.cloud import storage, logging
from google.api_core.exceptions import NotFound

from .settings import (
    AUTH0_CLIENT_ID,
    AUTH0_CLIENT_SECRET,
    AUTH0_DOMAIN,
    GOOGLE_LOGS_BUCKET,
)

MANAGEMENT_API = f"{AUTH0_DOMAIN}/api/v2/"
# Auth0's free tier expects no more than 2 requests/second.
# See: https://auth0.com/docs/policies/rate-limits
TIME_BETWEEN_REQUESTS = 0.5
LAST_LOG_ID = "auth0/__last_log_id.txt"


def store_auth0_logs(*args):
    """
    Get new access logs from Auth0 and store them in GCS.
    """
    # Get Auth0 management API access token
    print("Fetching Auth0 management API access token")
    token = _get_auth0_access_token()

    # Get last log ID from GCS
    print("Fetching ID of most recently saved Auth0 log")
    log_id = _get_last_auth0_log_id()

    # Get new access logs
    print(f"Fetching new Auth0 logs (since log with id {log_id})")
    logs = _get_new_auth0_logs(token, log_id)

    if len(logs) == 0:
        print(f"No new logs found (since log with id {log_id})")
        return

    # Send new access logs to StackDriver
    print(f"Sending new Auth0 logs to StackDriver")
    _send_new_auth0_logs_to_stackdriver(logs)

    # Save new access logs
    print(f"Saving new Auth0 logs to GCS")
    file_name = _save_new_auth0_logs(logs)

    print(f"New Auth0 logs saved to {file_name}")


def _get_auth0_access_token() -> str:
    """Fetches an access token for the Auth0 management API."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "audience": MANAGEMENT_API,
    }
    res = requests.post(f"{AUTH0_DOMAIN}/oauth/token", json=payload)
    return res.json()["access_token"]


__stackdriver_logger = None


def _get_stackdriver_logger():
    global __stackdriver_logger
    if __stackdriver_logger is None:
        client = logging.Client()
        __stackdriver_logger = client.logger("auth0")
    return __stackdriver_logger


__log_bucket = None


def _get_log_bucket():
    global __log_bucket
    if __log_bucket is None:
        client = storage.Client()
        __log_bucket = client.bucket(GOOGLE_LOGS_BUCKET)
    return __log_bucket


def _get_last_auth0_log_id() -> Optional[str]:
    """Fetch that ID of the last access log imported from Auth0"""
    blob = _get_log_bucket().get_blob(LAST_LOG_ID)
    if blob:
        return blob.download_as_string().decode("utf-8")
    return None


def _get_new_auth0_logs(token: str, log_id: Optional[str]) -> List[dict]:
    """Get all new access logs since `log_id`"""
    logs_endpoint = f"{MANAGEMENT_API}logs"
    headers = {"Authorization": f"Bearer {token}"}
    # see: https://auth0.com/docs/logs#get-logs-by-checkpoint
    params = {"take": 100}

    logs = []

    # Auth0 only returns at most 100 logs at a time, so we keep request
    # more logs until we stop receiving logs.
    while True:
        if log_id:
            params["from"] = log_id

        print(f"Fetching next logs batch: {params}")
        results = requests.get(logs_endpoint, headers=headers, params=params)
        gs_path = f"gs://{GOOGLE_LOGS_BUCKET}/auth0"

        if results.status_code != 200:
            raise Exception(
                f"Failed to fetch auth0 logs, Reason: {results.reason},"
                f" Status Code: {results.status_code}, Body:\n"
                f"{results.json()}"
            )

        new_logs = results.json()
        num_new_logs = len(new_logs)
        if num_new_logs == 0:
            print(f"Found no additional logs. Terminating request loop.")
            break

        print(f"Collected {num_new_logs} additional logs.")
        logs.extend(new_logs)
        log_id = new_logs[-1]["_id"]

        # Throttle our requests to the Auth0 API
        time.sleep(TIME_BETWEEN_REQUESTS)

    print(f"Collected {len(logs)} logs in total.")

    return logs


# Auth0 logging event codes
# see: https://auth0.com/docs/logs#log-data-event-listing
AUTH0_CODES_TO_USER_STR = {
    "seccft": "[access token]",  # client credentials grant success
    "f": "[no user info]",  # failed login
    "fsa": "[no user info]",  # failed silent auth
    "slo": "[no user info]",  # successful log out
}


def _send_new_auth0_logs_to_stackdriver(logs: List[dict]):
    """Log each new log to stackdriver for easy inspection"""
    logger = _get_stackdriver_logger()

    with logger.batch() as batch_logger:
        for log in logs:
            ts = datetime.strptime(log["date"], "%Y-%m-%dT%H:%M:%S.%fZ")

            event_type = log["type"]
            user_agent = log["user_agent"]
            user_name = log.get("user_name")

            # Depending on the event type, auth0 won't provide user
            # profile information, so we handle that here.
            if not user_name:
                user_name = AUTH0_CODES_TO_USER_STR.get(event_type, "[none]")

            extra_fields = {
                "__source": "auth0",
                "message": (
                    f"(EVENT={event_type})\t"
                    f"(USER={user_name})\t"
                    f"(USER_AGENT={user_agent})"
                ),
            }
            batch_logger.log_struct({**log, **extra_fields}, timestamp=ts)


def _save_new_auth0_logs(logs: List[dict]) -> str:
    """Save a list of access log objects from Auth0 to GCS"""
    # If a log ingestion fails one night, we might have logs for multiple days.
    # If so, we want to save those logs in separate GCS sub-buckets for each day.
    logs_by_date = {}
    for log in logs:
        date = datetime.strptime(log["date"], "%Y-%m-%dT%H:%M:%S.%fZ").date()
        logs_by_date[date] = logs_by_date.get(date, []) + [log]

    for date, daily_logs in logs_by_date.items():
        print(f"Saving Auth0 logs for {date} to GCS.")

        log_bucket = _get_log_bucket()

        # Save new logs to a blob in GCS.
        last_ts = datetime.strptime(daily_logs[-1]["date"], "%Y-%m-%dT%H:%M:%S.%fZ")
        logs_blob_name = f"{_get_logfile_name(last_ts)}.json"
        logs_blob = log_bucket.blob(f"auth0/{logs_blob_name}")
        logs_blob.upload_from_string(json.dumps(daily_logs))

    # Save the id of the most recent log in the collection.
    log_id = logs[-1]["_id"]
    id_blob = log_bucket.blob(LAST_LOG_ID)
    id_blob.upload_from_string(log_id)

    return f"gs://{logs_blob.bucket.name}/{logs_blob.name}"


def _get_logfile_name(dt: str = None):
    """Generate file name with structure [year]/[month]/[day]/[timestamp].json"""
    dt = str(dt or datetime.now())
    return dt.replace(" ", "/").replace("-", "/")
