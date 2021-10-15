from datetime import datetime
import logging
import sys
from typing import Any, Dict, Iterator

from .settings import ENV
from .util import (
    BackgroundContext,
    extract_pubsub_data,
    sqlalchemy_session,
)

from cidc_api.csms import get_with_paging
from cidc_api.models.templates.csms_api import (
    _extract_info_from_manifest,
    detect_manifest_changes,
    insert_manifest_from_json,
    insert_manifest_into_blob,
    NewManifestError,
)
from cidc_api.shared.gcloud_client import send_email
from cidc_api.shared.emails import CIDC_MAILING_LIST

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)

UPLOADER_EMAIL = ""


def update_cidc_from_csms(event: dict, context: BackgroundContext):
    """
    For every manifest in CSMS, detect changes using logic in API
    Does nothing if no changes are discovered, or if trial_id or manifest_id matching is not provided
    If it's a new manifest ie throws NewManifestError, put it through new manifest insert functions
    Send a singular email at the end with a description of the results of each new/changed manifest

    `event` data is a url params style dict eg trial_id=trial&manifest_id=manifest
        "trial_id" and "manifest_id" are used if provided to see whether a manifest should be processed
        special keyword "*" matches all 
        if one or both is not given, all manifests are considered matching that condition
    NOTE This handling should be reconsidered once all CIDC / CSMS data is aligned and we're out of testing
    """
    # TODO should we remove this handling once we're out of testing?
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict([tuple(kwarg.split("=")) for kwarg in data.split("&")])
    except Exception as e:
        # if anything errors, don't actually do any inserting
        data: dict = {}

    email_msg = []
    # TODO should we remove this handling once we're out of testing?
    if data and ("trial_id" not in data or "manifest_id" not in data):
        raise Exception(
            f"Both trial_id and manifest_id matching must be provided, you provided: {data}"
        )
    elif not data:
        if "data" in event:
            event["data"] = extract_pubsub_data(event)
        logger.warning(
            f"Both trial_id and manifest_id matching must be provided, no actual data changes will be made. Provided: {event!s}"
        )
        email_msg.append(
            f"Both trial_id and manifest_id matching must be provided, no actual data changes will be made. You provided: {event!s}"
        )

    with sqlalchemy_session() as session:
        manifest_iterator: Iterator[Dict[str, Any]] = get_with_paging("/manifests")

        for manifest in manifest_iterator:
            # TODO should we remove this handling once we're out of testing?
            # peeking ahead to check
            trial_id, manifest_id, _ = _extract_info_from_manifest(
                manifest, session=session
            )
            # when x not in passed data, data.get("x", x) == x so no manifests are skipped based on x
            if data.get("trial_id", trial_id) not in (trial_id, "*") or data.get(
                "manifest_id", manifest_id
            ) not in (manifest_id, "*"):
                continue

            try:
                detect_manifest_changes(
                    manifest, uploader_email=UPLOADER_EMAIL, session=session
                )

            except NewManifestError:

                if data:
                    # relational hook
                    insert_manifest_from_json(
                        manifest, uploader_email=UPLOADER_EMAIL, session=session
                    )

                    # schemas JSON blob hook
                    insert_manifest_into_blob(
                        manifest, uploader_email=UPLOADER_EMAIL, session=session
                    )

                    email_msg.append(
                        f"New {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                    )
                else:
                    email_msg.append(
                        f"Would add new {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                    )

            except Exception as e:
                email_msg.append(
                    f"Problem with {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}: {e!s}",
                )

        if email_msg:
            send_email(
                CIDC_MAILING_LIST,
                f"Summary of Update from CSMS: {datetime.now()}",
                "\n".join(email_msg),
            )
