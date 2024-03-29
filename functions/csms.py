from datetime import datetime
import logging
import sys
from typing import Any, Dict, Iterator
from urllib.parse import quote as url_escape

from .settings import ENV, INTERNAL_USER_EMAIL
from .util import BackgroundContext, extract_pubsub_data, sqlalchemy_session

from cidc_api.csms import get_with_paging
from cidc_api.models.csms_api import (
    _get_and_check,
    detect_manifest_changes,
    insert_manifest_into_blob,
    NewManifestError,
)
from cidc_api.shared.gcloud_client import send_email
from cidc_api.shared.emails import CIDC_MAILING_LIST

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


def update_cidc_from_csms(event: dict, context: BackgroundContext):
    """
    For every manifest in CSMS, detect changes using logic in API
    Does nothing if no changes are discovered, or if trial_id or manifest_id matching is not provided
    If it's a new manifest ie throws NewManifestError, put it through new manifest insert functions
    Send a singular email at the end with a description of the results of each new/changed manifest

    `event` data is base64-encoded str(dict) in the form {"trial_id": "<trial>", "manifest_id": "<manifest>"}
        values for "trial_id" and "manifest_id" are used to see whether a manifest should be processed
        special value "*" matches all
        fallback case if dict(data) raises error is dry run of all manifests
    NOTE This matching should be reconsidered once all CIDC / CSMS data is aligned and we're out of testing
    """
    dry_run: bool = True

    # TODO should we remove this matching once we're out of testing?
    try:
        # this returns the str, then convert it to a dict
        # uses event["data"] and then assumes format, so will error if no/malformatted data
        data: str = extract_pubsub_data(event)
        data: dict = dict(eval(data))
    except Exception as e:
        # if anything errors, don't actually do any inserting
        # just dry-run all of the manifest changes
        data: dict = {}

    else:
        if data:
            if "trial_id" not in data or "manifest_id" not in data:
                raise Exception(
                    f"trial_id and manifest_id matching must both be provided, you provided: {data}"
                )
            dry_run = False

    email_msg: str = []
    email_error: bool = False

    if dry_run:
        if "data" in event:
            try:
                # for human readability, but could be what errored above
                event["data"] = extract_pubsub_data(event)
            except Exception:
                pass

        logger.info(f"Dry-run call to update CIDC from CSMS. Provided: {event!s}")
        email_msg.append(
            f"To make changes, trial_id and manifest_id matching must both be provided in the event data. You provided: {event!s}"
        )

    else:
        logger.info(f"Call to update CIDC from CSMS matching: {data!s}")

    with sqlalchemy_session() as session:
        url = "/manifests"
        # only care about manifests that are qc_complete and non-"legacy" ie not excluded
        match_conditions = ["status=qc_complete", "excluded=false"]

        # TODO should we remove this matching once we're out of testing?
        # add matching conditions if not matching all
        if not dry_run and data["manifest_id"] != "*":
            match_conditions.append(f"manifest_id={url_escape(data['manifest_id'])}")

        url += "?" + "&".join(match_conditions)
        manifest_iterator: Iterator[Dict[str, Any]] = get_with_paging(url)

        for manifest in manifest_iterator:
            # TODO should we remove this matching once we're out of testing?
            samples = manifest.get("samples", [])
            try:
                trial_id = _get_and_check(
                    obj=samples,
                    key="protocol_identifier",
                    msg=f"No consistent protocol_identifier defined for samples on manifest {manifest.get('manifest_id')}",
                )
            except Exception as e:
                # if it doesn't have a consistent protocol_identifier, just log the error and skip it
                logger.error(str(e))
                continue

            # trial_id matching has to be done via _get_and_check as it is only stored on the samples
            if not dry_run and data["trial_id"] != "*" and trial_id != data["trial_id"]:
                logger.info(
                    f"Skipping manifest {manifest.get('manifest_id')} from {trial_id} != {data['trial_id']}"
                )
                continue

            try:
                # throws an error instead the catch if any change to critical fields, so we do need catch those too
                try:
                    # we're only dealing with new manifests, so we don't need to catch a change on any other manifest
                    changes = detect_manifest_changes(
                        manifest, uploader_email=INTERNAL_USER_EMAIL, session=session
                    )
                    # with updates within API's detect_manifest_changes() itself, we can capture
                    # # these changes and insert new manifests here, eliminating NewManifestError altogether

                except NewManifestError:
                    if data:
                        # schemas JSON blob hook
                        insert_manifest_into_blob(
                            manifest,
                            uploader_email=INTERNAL_USER_EMAIL,
                            dry_run=dry_run,
                            session=session,
                        )

                        logger.info(
                            f"New {trial_id} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                        )
                        email_msg.append(
                            f"New {trial_id} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                        )
                    else:
                        logger.info(
                            f"Would add new {trial_id} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                        )
                        email_msg.append(
                            f"Would add new {trial_id} manifest {manifest.get('manifest_id')} with {len(manifest.get('samples', []))} samples"
                        )

                else:
                    # TODO in the future, should actually handle chanes records above and handle
                    logger.info(
                        f"Changes found for {trial_id} manifest {manifest.get('manifest_id')}: {changes}"
                    )

            except Exception as e:
                logger.error(
                    f"Error with {trial_id} manifest {manifest.get('manifest_id')}: {e!r}"
                )
                email_msg.append(
                    f"Problem with {trial_id} manifest {manifest.get('manifest_id')}: {e!r}"
                )
                email_error = True

        if email_msg:
            logger.info(f"Email: {email_msg}")
            send_email(
                CIDC_MAILING_LIST,
                f"[DEV ALERT]({ENV})"
                + ("Error" if email_error else "Success")
                + f" updating from CSMS: {datetime.now()}",
                html_content="<br />".join(email_msg),
            )
