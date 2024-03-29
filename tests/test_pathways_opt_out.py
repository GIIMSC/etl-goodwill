import pandas as pd
import pytest
from sqlalchemy import select

from etl.utils.utils import make_dataframe_with_headers


def test_init_with_dataframe(opt_out):
    assert type(opt_out.dataframe_of_google_sheet) == pd.DataFrame


@pytest.mark.parametrize(
    "identifier, available_in_pathways, query_count",
    [("5f109a01-87c6", "", 0), ("663dfe-4aca", "No", 0), ("663dfe-4aca", "Yes", 1)],
)
def test_remove_programs_not_marked_for_pathways(
    identifier,
    available_in_pathways,
    query_count,
    opt_out,
    database_session,
    pathways_program_table,
    pathways_programs,
):
    select_stmt = select([pathways_program_table]).where(
        pathways_program_table.c.id == identifier
    )
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == 1

    opt_out.dataframe_of_google_sheet = make_dataframe_with_headers(
        [
            [
                "Should this program be available in Google Pathways?",
                "Row Identifier (DO NOT EDIT)",
            ],
            [available_in_pathways, identifier],
        ]
    )
    opt_out.remove_programs_not_marked_for_pathways()

    select_stmt = select([pathways_program_table]).where(
        pathways_program_table.c.id == identifier
    )
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == query_count


def test_remove_deleted_programs_no_deletion(
    opt_out, database_session, pathways_program_table, pathways_programs
):
    """Test that when the Google sheet has the same IDs as the database that no
    deletions occur.

    The `pathways_programs` fixture populates the database with two
    programs. This test instantiates Google Sheet data with those
    program IDs. Thus, `remove_deleted_programs` should not remove any
    entries from the database.
    """
    select_stmt = select([pathways_program_table])
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == 2

    google_sheet_as_list = [
        ["Row Identifier (DO NOT EDIT)"],
        ["5f109a01-87c6"],
        ["663dfe-4aca"],
    ]
    opt_out.dataframe_of_google_sheet = make_dataframe_with_headers(
        google_sheet_as_list
    )
    opt_out.remove_deleted_programs()

    select_stmt = select([pathways_program_table])
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == 2

    for program in query_results:
        assert program.id in ["5f109a01-87c6", "663dfe-4aca"]


def test_remove_deleted_programs_one_deletion(
    opt_out, database_session, pathways_program_table, pathways_programs
):
    """Test that when the database has IDs not in the Google Sheet
    `remove_deleted_programs` removes those IDs.

    The `pathways_programs` fixture populates the database with two
    programs. This test instantiates Google Sheet data with only ONE of
    the program IDs. Thus, `remove_deleted_programs` should not remove
    one entry from the database.
    """
    select_stmt = select([pathways_program_table])
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == 2

    google_sheet_as_list = [["Row Identifier (DO NOT EDIT)"], ["5f109a01-87c6"]]
    opt_out.dataframe_of_google_sheet = make_dataframe_with_headers(
        google_sheet_as_list
    )
    opt_out.remove_deleted_programs()

    select_stmt = select([pathways_program_table])
    query_results = database_session.execute(select_stmt).fetchall()

    assert len(query_results) == 1

    for program in query_results:
        assert program.id in ["5f109a01-87c6"]
