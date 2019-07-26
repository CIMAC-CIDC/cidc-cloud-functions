import pytest

from functions.util import get_db_session


@pytest.fixture
def db_session():
    return get_db_session()
