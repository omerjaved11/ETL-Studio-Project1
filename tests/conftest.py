# tests/conftest.py
import pandas as pd
import pytest


@pytest.fixture
def sample_df():
    """
    Small DataFrame for pipeline/db tests.
    """
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10.5, None, 30.0],
            "flag": [True, False, True],
        }
    )
