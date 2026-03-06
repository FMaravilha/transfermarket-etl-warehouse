from datetime import datetime
from src.extract.download_dataset import parse_remote_version

def test_parse_remote_version():

    fake_kaggle_output = """
    ref                           title                         size      lastUpdated
    davidcariboo/player-scores    Football Data                 193726663 2026-02-27 05:43:35.993000
    """

    version = parse_remote_version(fake_kaggle_output)

    assert version == datetime(2026, 2, 27, 5, 43, 35, 993000)

    

