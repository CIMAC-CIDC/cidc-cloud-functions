from tests.util import make_pubsub_event
from functions.util import extract_pubsub_data


def test_extract_pubsub_data():
    """Ensure that extract_pubsub_data can do what it claims"""
    data = "hello there"
    event = make_pubsub_event(data)
    assert extract_pubsub_data(event) == data
