import os
from io import BytesIO, StringIO
from unittest.mock import MagicMock

import pytest
import pandas as pd

import functions.visualizations
from functions.visualizations import (
    vis_preprocessing,
    DownloadableFiles,
    _ClustergrammerTransform,
    _npx_to_dataframe,
)

from tests.util import make_pubsub_event

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
NPX_PATH = os.path.join(DATA_DIR, "fake_npx.xlsx")


@pytest.fixture
def metadata_df():
    """Fake participants/sample metadata for `fake_npx.xlsx`"""
    metadata_df = pd.DataFrame.from_dict(
        {
            "cimac_id": ["CTTTTPPS1.01", "CTTTTPPS2.01"],
            "cimac_participant_id": ["CTTTTPP", "CTTTTPP"],
            "cohort_name": ["Arm_A", "Arm_A"],
            "collection_event_name": ["Event1", "Event2"],
        }
    )
    metadata_df.set_index("cimac_id", inplace=True)
    return metadata_df


def test_npx_clustergrammer_end_to_end(monkeypatch, metadata_df):
    """Test the NPX-clustergrammer transform."""
    # Test no file found
    monkeypatch.setattr(
        DownloadableFiles, "get_by_object_url", lambda *args, **kwargs: None
    )
    with pytest.raises(Exception, match="No downloadable file"):
        vis_preprocessing(make_pubsub_event("foo/bar"), {})

    # Mock an NPX downloadable file record
    npx_record = MagicMock()
    npx_record.object_url = "foo"
    npx_record.data_format = "NPX"
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = npx_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    gcs_blob = MagicMock()
    _get_blob_as_stream = MagicMock()
    fake_npx = open(NPX_PATH, "rb")
    _get_blob_as_stream.return_value = fake_npx
    monkeypatch.setattr(
        functions.visualizations, "get_blob_as_stream", _get_blob_as_stream
    )

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_blob_as_stream.assert_called_once()
    _get_metadata_df.assert_called_once()

    # Check contents of the clustergrammer output
    row_names = [row["name"] for row in npx_record.clustergrammer["row_nodes"]]
    col_names = [col["name"] for col in npx_record.clustergrammer["col_nodes"]]
    col_cats = [
        (col["cat-0"], col["cat-1"], col["cat-2"])
        for col in npx_record.clustergrammer["col_nodes"]
    ]

    # Based on the contents of fake_npx.xlsx...
    assert row_names == ["Assay1", "Assay2"]
    assert col_names == [
        "CIMAC Sample ID: CTTTTPPS1.01",
        "CIMAC Sample ID: CTTTTPPS2.01",
    ]

    # Based on the construction of metadata_df...
    assert col_cats == [
        ("Participant ID: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event1"),
        ("Participant ID: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event2"),
    ]

    fake_npx.close()


def test_clustergrammer_transform(metadata_df):
    """Smoke test for the clustergrammer transform"""
    cg = _ClustergrammerTransform()

    with open(NPX_PATH, "rb") as fake_npx:
        assert isinstance(cg.npx(fake_npx, metadata_df), dict)


def test_npx_to_dataframe():
    """Extract data from a fake NPX file"""
    with open(NPX_PATH, "rb") as fake_npx:
        npx_df = _npx_to_dataframe(fake_npx)

    expected_df = pd.DataFrame(
        {"CTTTTPPS1.01": [1, -1], "CTTTTPPS2.01": [-1, 1]}, index=["Assay1", "Assay2"]
    )
    assert expected_df.equals(npx_df)
