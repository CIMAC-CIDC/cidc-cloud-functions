import os
from io import StringIO
from unittest.mock import MagicMock

import pytest
import pandas as pd

import functions.visualizations
from functions.visualizations import (
    vis_preprocessing,
    DownloadableFiles,
    TrialMetadata,
    _ClustergrammerTransform,
    _cytof_summary_to_dataframe,
    _npx_to_dataframe,
    _metadata_to_categories,
    _add_antibody_metadata,
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
            "collection_event_name": ["Event1", "Event2"],  # -> CG 'Clin benefit'
        }
    )
    metadata_df.set_index("cimac_id", inplace=True)
    return metadata_df


def test_loading_lazily(monkeypatch, metadata_df):
    """Test that files aren't loaded if there are no transformations for them"""
    record = MagicMock()
    record.object_url = "foo.txt"
    record.upload_type = "something"
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


def test_add_antibody_metadata_validation(monkeypatch, metadata_df):
    """Test that the validation checks in _add_antibody_metadata throw errors as expected"""
    record = MagicMock()
    record.object_url = "foo.txt"
    record.upload_type = "mif"

    ct_typeerror = {"assays": {"mif": 5}}
    trial_md = MagicMock()
    trial_md.metadata_json = ct_typeerror
    get_trial_by_id = MagicMock()
    get_trial_by_id.return_value = trial_md
    with monkeypatch.context() as m:
        m.setattr(TrialMetadata, "find_by_trial_id", get_trial_by_id)
        with pytest.raises(TypeError, match="Issue loading antibodies"):
            _add_antibody_metadata(record, metadata_df)

    ct_nonunique = {"assays": {"mif": ["foo.txt", "foo.txt"]}}
    get_trial_by_id.return_value.metadata_json = ct_nonunique
    with monkeypatch.context() as m:
        m.setattr(TrialMetadata, "find_by_trial_id", get_trial_by_id)
        with pytest.raises(Exception, match="Issue loading antibodies"):
            _add_antibody_metadata(record, metadata_df)


def test_cytof_antibody_metadata_end_to_end(monkeypatch, metadata_df):
    """Test addition of antibody metadata for cytof files"""
    # Mock an CyTOF downloadable file record
    cytof_record = MagicMock()
    cytof_record.object_url = "foo.txt"
    cytof_record.upload_type = "cytof"
    cytof_record.additional_metadata = {"foo": "bar"}
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = cytof_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock trial
    ct = MagicMock()
    ct.metadata_json = {
        "assays": {
            "cytof": [
                {
                    "cytof_antibodies": [
                        {"usage": "Ignored"},
                        {
                            "usage": "Used",
                            "stain_type": "Surface Stain",
                            "isotope": "000Foo",
                            "antibody": "Bar",
                            "clone": "Nx/xxx",
                        },
                        {
                            "usage": "Analysis Only",
                            "stain_type": "Intracellular",
                            "isotope": "001Foo",
                            "antibody": "Baz",
                        },
                    ],
                    "object_url": "foo.txt",  # for DeepSearch
                }
            ]
        }
    }
    find_by_trial_id = MagicMock()
    find_by_trial_id.return_value = ct
    monkeypatch.setattr(TrialMetadata, "find_by_trial_id", find_by_trial_id)

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_metadata_df.assert_called_once()

    assert cytof_record.additional_metadata == {
        "foo": "bar",
        "cytof.antibodies": "surface 000Foo-Bar (Nx/xxx), intracellular 001Foo-Baz",
    }


def test_elisa_antibody_metadata_end_to_end(monkeypatch, metadata_df):
    """Test addition of antibody metadata for ELISA files"""
    # Mock an ELISA downloadable file record
    elisa_record = MagicMock()
    elisa_record.object_url = "foo.txt"
    elisa_record.upload_type = "elisa"
    elisa_record.additional_metadata = {"foo": "bar"}
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = elisa_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    ct = MagicMock()
    ct.metadata_json = {
        "assays": {
            "elisa": [
                {
                    "antibodies": [
                        {"usage": "Ignored"},
                        {
                            "usage": "Used",
                            "stain_type": "Surface Stain",
                            "isotope": "000Foo",
                            "antibody": "Bar",
                            "clone": "Nx/xxx",
                        },
                        {
                            "usage": "Analysis Only",
                            "stain_type": "Intracellular",
                            "isotope": "001Foo",
                            "antibody": "Baz",
                        },
                    ],
                    "object_url": "foo.txt",  # for DeepSearch
                }
            ]
        }
    }
    find_by_trial_id = MagicMock()
    find_by_trial_id.return_value = ct
    monkeypatch.setattr(TrialMetadata, "find_by_trial_id", find_by_trial_id)

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_metadata_df.assert_called_once()

    assert elisa_record.additional_metadata == {
        "foo": "bar",
        "elisa.antibodies": "surface 000Foo-Bar (Nx/xxx), intracellular 001Foo-Baz",
    }


def test_ihc_antibody_metadata_end_to_end(monkeypatch, metadata_df):
    """Test addition of antibody metadata for IHC files"""
    # Mock an IHC downloadable file record
    ihc_record = MagicMock()
    ihc_record.object_url = "foo.txt"
    ihc_record.upload_type = "ihc"
    ihc_record.additional_metadata = {"foo": "bar"}
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = ihc_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    ct = MagicMock()
    ct.metadata_json = {
        "assays": {
            "ihc": [
                {
                    "antibody": {"antibody": "Bar", "clone": "Nx/xxx"},
                    "object_url": "foo.txt",  # for DeepSearch
                }
            ]
        }
    }
    find_by_trial_id = MagicMock()
    find_by_trial_id.return_value = ct
    monkeypatch.setattr(TrialMetadata, "find_by_trial_id", find_by_trial_id)

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_metadata_df.assert_called_once()

    assert ihc_record.additional_metadata == {
        "foo": "bar",
        "ihc.antibody": "Bar (Nx/xxx)",
    }


def test_micsss_antibody_metadata_end_to_end(monkeypatch, metadata_df):
    """Test addition of antibody metadata for MICSSS files"""
    # Mock an MICSSS downloadable file record
    micsss_record = MagicMock()
    micsss_record.object_url = "foo.txt"
    micsss_record.upload_type = "micsss"
    micsss_record.additional_metadata = {"foo": "bar"}
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = micsss_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    ct = MagicMock()
    ct.metadata_json = {
        "assays": {
            "micsss": [
                {
                    "antibody": [
                        {"antibody": "Bar", "clone": "Nx/xxx"},
                        {"antibody": "Baz"},
                    ],
                    "object_url": "foo.txt",  # for DeepSearch
                }
            ]
        }
    }
    find_by_trial_id = MagicMock()
    find_by_trial_id.return_value = ct
    monkeypatch.setattr(TrialMetadata, "find_by_trial_id", find_by_trial_id)

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_metadata_df.assert_called_once()

    assert micsss_record.additional_metadata == {
        "foo": "bar",
        "micsss.antibodies": "Bar (Nx/xxx), Baz",
    }


def test_mif_antibody_metadata_end_to_end(monkeypatch, metadata_df):
    """Test addition of antibody metadata for MIF files"""
    # Mock an MIF downloadable file record
    mif_record = MagicMock()
    mif_record.object_url = "foo.txt"
    mif_record.upload_type = "mif"
    mif_record.additional_metadata = {"foo": "bar"}
    get_by_object_url = MagicMock()
    get_by_object_url.return_value = mif_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", get_by_object_url)

    # Mock GCS call
    ct = MagicMock()
    ct.metadata_json = {
        "assays": {
            "mif": [
                {
                    "antibodies": [
                        {"export_name": "Foo"},
                        {"antibody": "Bar", "clone": "Nx/xxx", "fluor_wavelength": 500},
                        {"antibody": "Baz", "fluor_wavelength": 500},
                    ],
                    "object_url": "foo.txt",  # for DeepSearch
                }
            ]
        }
    }
    find_by_trial_id = MagicMock()
    find_by_trial_id.return_value = ct
    monkeypatch.setattr(TrialMetadata, "find_by_trial_id", find_by_trial_id)

    # Mock metadata_df
    _get_metadata_df = MagicMock()
    _get_metadata_df.return_value = metadata_df
    monkeypatch.setattr(functions.visualizations, "_get_metadata_df", _get_metadata_df)

    vis_preprocessing(make_pubsub_event("1"), {})
    get_by_object_url.assert_called_once()
    _get_metadata_df.assert_called_once()

    assert mif_record.additional_metadata == {
        "foo": "bar",
        "mif.antibodies": "Foo, Bar (Nx/xxx - 500), Baz (500)",
    }


def test_ihc_combined_end_to_end(monkeypatch, metadata_df):
    """Test the IHC combined transform."""
    # Mock an IHC combined downloadable file record
    ihc_record = MagicMock()
    ihc_record.object_url = "foo.txt"
    ihc_record.upload_type = "ihc marker combined"
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
    npx_record.object_url = "npx.xlsx"
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
        ("Participant Id: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event1"),
        ("Participant Id: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event2"),
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
    cytof_record.object_url = "foo.txt"
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
        ("Participant Id: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event1"),
        ("Participant Id: CTTTTPP", "Cohort: Arm_A", "Collection Event: Event2"),
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
            "Treatment: False",
            "Collection Event: c",
            "Cohort: b",
            "Clin benefit: z",
            "Participant Id: a",
        ),
        (
            "CIMAC Id: CT2",
            "Treatment: True",
            "Collection Event: f",
            "Cohort: e",
            "Clin benefit: y",
            "Participant Id: d",
        ),
        (
            "CIMAC Id: CT3",
            "Treatment: True",
            "Collection Event: i",
            "Cohort: h",
            "Clin benefit: x",
            "Participant Id: g",
        ),
        (
            "CIMAC Id: CT4",
            "Treatment: False",
            "Collection Event: c",
            "Cohort: e",
            "Clin benefit: x",
            "Participant Id: j",
        ),
    ]

    categories = _metadata_to_categories(md_names)
    assert cat_names == categories
