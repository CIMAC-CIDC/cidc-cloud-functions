from typing import Any, Dict, Iterator

from .util import sqlalchemy_session

from cidc_api.csms import get_with_paging
from cidc_api.models.templates.csms_api import (
    detect_manifest_changes,
    insert_manifest_from_json,
    insert_manifest_into_blob,
    NewManifestError,
)
from cidc_api.shared.gcloud_client import send_email
from cidc_api.shared.emails import CIDC_MAILING_LIST

UPLOADER_EMAIL = ""


def update_cidc_from_csms(*args):
    """"""
    with sqlalchemy_session() as session:
        manifest_iterator: Iterator[Dict[str, Any]] = get_with_paging("/manifests")

        for manifest in manifest_iterator:
            try:
                records, changes = detect_manifest_changes(
                    manifest, uploader_email=UPLOADER_EMAIL, session=session
                )

            except NewManifestError:
                # relational hook
                insert_manifest_from_json(
                    manifest, uploader_email=UPLOADER_EMAIL, session=session
                )

                # schemas JSON blob hook
                insert_manifest_into_blob(
                    manifest, uploader_email=UPLOADER_EMAIL, session=session
                )

                send_email(
                    CIDC_MAILING_LIST,
                    f"Changes for {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}",
                    f"New manifest with {len(manifest.get('samples', []))} samples",
                )

            except Exception as e:
                send_email(
                    CIDC_MAILING_LIST,
                    f"Problem with {manifest.get('protocol_identifier')} manifest {manifest.get('manifest_id')}",
                    str(e),
                )
