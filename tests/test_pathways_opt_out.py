import pandas as pd


def test_init_with_dataframe(opt_out):
    assert type(opt_out.dataframe_of_google_sheet) == pd.DataFrame
