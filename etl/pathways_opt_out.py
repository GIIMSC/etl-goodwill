from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select

from etl.utils.utils import make_dataframe_with_headers


def remove_deleted_programs(list_of_lists, programs_table, engine):
    """Delete programs from the database.

    This function deletes programs from the database, if those programs
    no longer reside in the Master Google Sheet. This may happen when a local Goodwill
    outright deletes a program from their local sheet.

    Args:
        list_of_lists: all Google sheet data; return value of `get_sheet_as_list` function in Extractor
        engine: a SQLAlchemy engine
    """
    # 1. Get all IDs in the Google sheet by filtering a Pandas dataframe.
    dataframe = make_dataframe_with_headers(list_of_lists)
    all_ids_in_sheet = dataframe["Row Identifier (DO NOT EDIT)"].tolist()

    with engine.connect() as connection:
        # 2. Get all IDs in the Pathways database
        select_all = select([programs_table])
        all_programs_in_database = connection.execute(select_all)
        all_ids_in_database = [program.id for program in all_programs_in_database]

        # 3. Determine if the database has IDs that the Google does not have
        programs_to_delete = [
            program_id
            for program_id in all_ids_in_database
            if program_id not in all_ids_in_sheet
        ]

        # 4. (OPTIONAL) Delete programs from Pathways database
        if programs_to_delete:
            delete_object = programs_table.delete().where(
                programs_table.c.id.in_(program for program in programs_to_delete)
            )
            connection.execute(delete_object)

    return programs_to_delete


def remove_programs_not_marked_for_pathways(dataframe):

    """
    1. filter dataframe for "no"

    self.dataframe[
        self.dataframe["Should this program be available in Google Pathways?"]
        == "Yes"
    ]

    2. get the IDs

    3. delete those IDs, if they exist, from the pathways database.
    """

    return
