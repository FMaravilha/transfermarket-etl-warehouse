import pytest
import pandas as pd
from src.validate.validate_raw import validate_file

def test_validate_missing_columns(tmp_path):

    # criar CSV com coluna errada
    df = pd.DataFrame({
        "club_id": [1, 2, 3]
    })

    file = tmp_path / "clubs.csv"
    df.to_csv(file, index=False)

    required_columns = {"club_id", "name"}

    with pytest.raises(ValueError):
        validate_file(file, required_columns)

def test_empty_file(tmp_path):

    df = pd.DataFrame()

    file = tmp_path / "clubs.csv"
    df.to_csv(file, index=False)

    required_columns = {"club_id", "name"}

    with pytest.raises(ValueError):
        validate_file(file, required_columns)

