import pandas as pd

from etl.utils.utils import make_dataframe_with_headers
from tests.models_for_testing import PathwaysProgram


def test_init_with_dataframe(opt_out):
    assert type(opt_out.dataframe_of_google_sheet) == pd.DataFrame


def test_remove_deleted_program(opt_out, database_session, pathways_programs):
    query_results = database_session.query(PathwaysProgram).filter(
        PathwaysProgram.id == "663dfe-4aca"
    )
    assert len(query_results.all()) == 1

    opt_out.dataframe_of_google_sheet = make_dataframe_with_headers(
        [
            [
                "Should this program be available in Google Pathways?",
                "Row Identifier (DO NOT EDIT)",
            ],
            ["No", "663dfe-4aca"],
        ]
    )
    opt_out.remove_programs_not_marked_for_pathways()

    query_results = database_session.query(PathwaysProgram).filter(
        PathwaysProgram.id == "663dfe-4aca"
    )
    assert len(query_results.all()) == 0


# Let's standup a test database...
# test_remove_deleted_program
# it deletes a program – the Google sheet is missing a program in the database
# it delete more than one program – the Google sheet is missing multiple programs in the database
# it does not delete any programs -
