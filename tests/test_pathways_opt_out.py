import pandas as pd


def test_init_with_dataframe(opt_out):
    assert type(opt_out.dataframe_of_google_sheet) == pd.DataFrame


# Let's standup a test database...
# test_remove_deleted_program
# it deletes a program – the Google sheet is missing a program in the database
# it delete more than one program – the Google sheet is missing multiple programs in the database
# it does not delete any programs -
