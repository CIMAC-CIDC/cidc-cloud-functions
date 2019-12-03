import os
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


def test_vis_preprocessing(monkeypatch):
    """Test control flow for the vis_preprocessing function"""
    # Test no file found
    monkeypatch.setattr(DownloadableFiles, "find_by_id", lambda *args, **kwargs: None)
    with pytest.raises(Exception, match="No downloadable file"):
        vis_preprocessing(make_pubsub_event("1"), {})

    # Mock an NPX downloadable file record
    npx_record = MagicMock()
    npx_record.object_url = "foo"
    npx_record.data_format = "NPX"
    find_by_id = MagicMock()
    find_by_id.return_value = npx_record
    monkeypatch.setattr(DownloadableFiles, "find_by_id", find_by_id)

    # Mock GCS call
    gcs_blob = MagicMock()
    get_file_from_gcs = MagicMock()
    get_file_from_gcs.return_value = gcs_blob
    monkeypatch.setattr(
        functions.visualizations, "_get_file_from_gcs", get_file_from_gcs
    )

    # Mock Clustgrammer preprocessing
    cg_json = {"foo": "bar"}
    cg_npx_transform = MagicMock()
    cg_npx_transform.return_value = cg_json
    monkeypatch.setattr(_ClustergrammerTransform, "npx", cg_npx_transform)

    vis_preprocessing(make_pubsub_event("1"), {})
    find_by_id.assert_called_once()
    get_file_from_gcs.assert_called_once()

    assert npx_record.clustergrammer == cg_json


def test_npx_to_dataframe():
    """Extract data from a fake NPX file"""
    with open(os.path.join(DATA_DIR, "fake_npx.xlsx"), "rb") as fake_npx:
        npx_df = _npx_to_dataframe(fake_npx)

    expected_df = pd.DataFrame(
        {"CTTTTPPS1.01": [1, -1], "CTTTTPPS2.01": [-1, 1]}, index=["Assay1", "Assay2"]
    )
    assert expected_df.equals(npx_df)
