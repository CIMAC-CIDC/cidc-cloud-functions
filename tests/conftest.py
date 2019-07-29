import pytest

from functions.util import get_db_session


# TODO: set up database migrations for this project
# so that tests can actually modify the test database instance.
@pytest.fixture
def db_session():
    return get_db_session()
