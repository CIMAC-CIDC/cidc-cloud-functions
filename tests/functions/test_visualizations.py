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
    _cytof_summary_to_dataframe,
    _npx_to_dataframe,
    _metadata_to_categories,
)

from tests.util import make_pubsub_event

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
NPX_PATH = os.path.join(DATA_DIR, "fake_npx.xlsx")
CYTOF_PATH = os.path.join(DATA_DIR, "fake_cytof_summary.csv")


@pytest.fixture
def metadata_df():
    """Fake participants/sample metadata for `fake_npx.xlsx`"""
    # if three or fewer columns, doesn't try to combine anything when generating CG categories
    metadata_df = pd.DataFrame.from_dict(
        {
            "cimac_id": ["CTTTTPPS1.01", "CTTTTPPS2.01"],  # index -> CG 'CIMAC Id'
            "cimac_participant_id": ["CTTTTPP", "CTTTTPP"],  # -> CG 'Participant Id'
            "cohort_name": [
                "Arm_A",
                "Arm_A",
            ],  # kept no matter cardinality; -> CG 'Cohort'
            "collection_event_name": [
                "Event1",
                "Event2",
            ],  # -> CG 'RECIST clinical benefit status'
        }
    )
    metadata_df.set_index("cimac_id", inplace=True)
    return metadata_df


def test_loading_lazily(monkeypatch, metadata_df):
    """Test that files aren't loaded if there are no transformations for them"""
    record = MagicMock()
    record.object_url = "foo"
    record.upload_type = "something"
    record.data_format = "CSV"
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    get_blob_as_stream = MagicMock()
    monkeypatch.setattr(
        functions.visualizations, "get_blob_as_stream", get_blob_as_stream
    )

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_blob_as_stream.assert_not_called()


def test_ihc_combined_end_to_end(monkeypatch, metadata_df):
    """Test the IHC combined transform."""
    # Mock an IHC combined downloadable file record
    ihc_record = MagicMock()
    ihc_record.object_url = "foo"
    ihc_record.upload_type = "ihc marker combined"
    ihc_record.data_format = "CSV"
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = ihc_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    gcs_blob = MagicMock()
    _get_blob_as_stream = MagicMock()
    combined_csv = StringIO("cimac_id,foo,bar\nCTTTTPPS1.01,1,2\nCTTTTPPS2.01,3,4")
    _get_blob_as_stream.return_value = combined_csv
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

    assert ihc_record.ihc_combined_plot == [
        {
            "cimac_id": "CTTTTPPS1.01",
            "foo": 1,
            "bar": 2,
            "cimac_participant_id": "CTTTTPP",
            "cohort_name": "Arm_A",
            "collection_event_name": "Event1",
        },
        {
            "cimac_id": "CTTTTPPS2.01",
            "foo": 3,
            "bar": 4,
            "cimac_participant_id": "CTTTTPP",
            "cohort_name": "Arm_A",
            "collection_event_name": "Event2",
        },
    ]


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
    assert col_names == ["CIMAC Id: CTTTTPPS1.01", "CIMAC Id: CTTTTPPS2.01"]

    # Based on the construction of metadata_df...
    assert col_cats == [
        (
            "Participant Id: CTTTTPP",
            "Collection Event: Event1",
            "Cohort: Arm_A",
        ),
        (
            "Participant Id: CTTTTPP",
            "Collection Event: Event2",
            "Cohort: Arm_A",
        ),
    ]

    fake_npx.close()


@pytest.mark.parametrize(
    "upload_type",
    ("cell counts compartment", "cell counts assignment", "cell counts profiling"),
)
def test_cytof_clustergrammer_end_to_end(monkeypatch, metadata_df, upload_type):
    """Test the CyTOF-clustergrammer transform."""
    # Test no file found
    monkeypatch.setattr(
        DownloadableFiles, "get_by_object_url", lambda *args, **kwargs: None
    )
    with pytest.raises(Exception, match="No downloadable file"):
        vis_preprocessing(make_pubsub_event("foo/bar"), {})

    # Mock a CyTOF summary downloadable file record
    cytof_record = MagicMock()
    cytof_record.object_url = "foo"
    cytof_record.upload_type = upload_type
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = cytof_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    gcs_blob = MagicMock()
    _get_blob_as_stream = MagicMock()
    fake_cytof = open(CYTOF_PATH, "rb")
    _get_blob_as_stream.return_value = fake_cytof
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
    row_names = [row["name"] for row in cytof_record.clustergrammer["row_nodes"]]
    col_names = [col["name"] for col in cytof_record.clustergrammer["col_nodes"]]
    col_cats = [
        (col["cat-0"], col["cat-1"], col["cat-2"])
        for col in cytof_record.clustergrammer["col_nodes"]
    ]

    # Based on the contents of fake_cytof_summary.csv...
    assert row_names == ["cell1", "cell2"]
    assert col_names == ["CIMAC Id: CTTTTPPS1.01", "CIMAC Id: CTTTTPPS2.01"]

    # Based on the construction of metadata_df...
    assert col_cats == [
        (
            "Participant Id: CTTTTPP",
            "Collection Event: Event1",
            "Cohort: Arm_A",
        ),
        (
            "Participant Id: CTTTTPP",
            "Collection Event: Event2",
            "Cohort: Arm_A",
        ),
    ]

    fake_cytof.close()


def test_cytof_to_dataframe():
    """Extract data from a CyTOF summary CSV"""
    with open(CYTOF_PATH, "rb") as fake_cytof:
        cytof_df = _cytof_summary_to_dataframe(fake_cytof)

    expected_df = pd.DataFrame(
        {"CTTTTPPS1.01": [1, 2], "CTTTTPPS2.01": [2, 1]}, index=["cell1", "cell2"]
    )
    assert expected_df.equals(cytof_df)


def test_npx_to_dataframe():
    """Extract data from a fake NPX file"""
    with open(NPX_PATH, "rb") as fake_npx:
        npx_df = _npx_to_dataframe(fake_npx)

    expected_df = pd.DataFrame(
        {"CTTTTPPS1.01": [1, -1], "CTTTTPPS2.01": [-1, 1]}, index=["Assay1", "Assay2"]
    )
    assert expected_df.equals(npx_df)


def test_clustergrammerify_single_sample(metadata_df):
    """Ensure an assertion error gets raised if only one sample is passed to clustergrammerify"""
    cg = _ClustergrammerTransform()

    data_df = pd.DataFrame({"CTTTPPS1.01": [1]}, index=["row1"])

    with pytest.raises(AssertionError, match="with only one sample"):
        cg._clustergrammerify(data_df, metadata_df)


def test_metadata_to_categories():
    # Converts names as expected
    md_names = pd.DataFrame(
        [
            ["CT1", "a", "b", "c", 0, "z"],
            ["CT2", "d", "e", "f", 1, "y"],
            ["CT3", "g", "h", "i", 1, "x"],
            ["CT4", "j", "e", "c", 0, "x"],
        ],
        columns=[
            "cimac_id",  # new index
            "cimac_participant_id",  # 'CIMAC' dropped
            "cohort_name",  # 'name' dropped
            "arbitrary_trial_specific_clinical_annotations.Collection Event (days)",  # front stripped, parentheses dropped; same casing
            "arbitrary_trial_specific_clinical_annotations.Treatment (1=Yes,0=No)",  # for Title case, under to spaces without intro
            "arbitrary_trial_specific_clinical_annotations.RECIST clinical benefit status",  # front stripped
        ],
    )
    md_names.set_index("cimac_id", inplace=True)
    cat_names = [
        (
            "CIMAC Id: CT1",
            "Participant Id: a",
            "Collection Event: c",
            "Cohort: b",
            "Treatment: False",
            "RECIST clinical benefit status: z",
        ),
        (
            "CIMAC Id: CT2",
            "Participant Id: d",
            "Collection Event: f",
            "Cohort: e",
            "Treatment: True",
            "RECIST clinical benefit status: y",
        ),
        (
            "CIMAC Id: CT3",
            "Participant Id: g",
            "Collection Event: i",
            "Cohort: h",
            "Treatment: True",
            "RECIST clinical benefit status: x",
        ),
        (
            "CIMAC Id: CT4",
            "Participant Id: j",
            "Collection Event: c",
            "Cohort: e",
            "Treatment: False",
            "RECIST clinical benefit status: x",
        ),
    ]

    categories = _metadata_to_categories(md_names)
    assert cat_names == categories
