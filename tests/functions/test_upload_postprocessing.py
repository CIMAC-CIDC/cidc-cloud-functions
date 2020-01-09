from unittest.mock import MagicMock

import pytest
from cidc_api.models import AssayUploads, AssayUploadStatus

from functions import upload_postprocessing

from tests.util import make_pubsub_event, with_app_context

UPLOAD_ID = 1


def test_derive_files_from_upload(monkeypatch):
    event = make_pubsub_event(str(UPLOAD_ID))
    upload = AssayUploads(id=UPLOAD_ID, status=AssayUploadStatus.MERGE_FAILED.value)

    find_by_id = MagicMock()
    find_by_id.return_value = None
    monkeypatch.setattr(AssayUploads, "find_by_id", find_by_id)

    with pytest.raises(Exception, match="No upload record with id"):
        upload_postprocessing.derived_files_from_upload(event, None)

    find_by_id.return_value = upload
    with pytest.raises(Exception, match="status is merge-failed"):
        upload_postprocessing.derived_files_from_upload(event, None)
