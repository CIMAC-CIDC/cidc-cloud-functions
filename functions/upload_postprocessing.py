"""Functions that perform postprocessing after an upload completes."""
from cidc_api.models import AssayUploads, AssayUploadStatus

from .settings import GOOGLE_DATA_BUCKET
from .util import (
    BackgroundContext,
    extract_pubsub_data,
    sqlalchemy_session,
    get_blob_as_stream,
)


def derived_files_from_upload(event: dict, context: BackgroundContext):
    with sqlalchemy_session() as session:
        upload_id = extract_pubsub_data(event)
        upload_record: AssayUploads = AssayUploads.find_by_id(upload_id)

        if not upload_record:
            raise Exception(f"No upload record with id {upload_id} found.")

        if AssayUploadStatus(upload_record.status) != AssayUploadStatus.MERGE_COMPLETED:
            raise Exception(
                f"Cannot perform postprocessing on upload {upload_id}: status is {upload_record.status}"
            )

        # TODO: upload_record info and execution context to a
        # not-yet-implemented `unprism.derive_files` function, or similar.
        print(
            f"Received completed upload {upload_id} for postprocessing. (NOT YET IMPLEMENTED)"
        )
