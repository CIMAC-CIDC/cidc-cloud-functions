from unittest.mock import MagicMock

import pytest

import functions.visualizations
from functions.visualizations import (
    vis_preprocessing,
    DownloadableFiles,
    _ClustergrammerTransform,
)

from tests.util import make_pubsub_event


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


def test_clustergrammer_npx_transform():
    """TODO"""
