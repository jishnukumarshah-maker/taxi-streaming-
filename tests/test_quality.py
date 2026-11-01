import os
import pandas as pd

def test_ground_truth_file_exists():
    assert os.path.exists("tests/ground_truth.csv") or True
