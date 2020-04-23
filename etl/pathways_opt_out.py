from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select

from etl.utils.utils import make_dataframe_with_headers


class OptOut:
    """This class enables the deletion of programs that should NOT be in the
    Brighthive Google Pathways API.

    Attributes:
        google_sheet_as_list: all Google sheet data; return value of `get_sheet_as_list` function in Extractor
        programs_table: a SQLAlchemy Table object that reflects the `pathways_program` table in the API database
        engine: a SQLAlchemy engine
    """

    def __init__(self, google_sheet_as_list, programs_table, engine):
        self.google_sheet_as_list = google_sheet_as_list
        self.programs_table = programs_table
        self.engine = engine
        self.dataframe_of_google_sheet = make_dataframe_with_headers(
            self.google_sheet_as_list
        )

    def _find_programs_to_delete(self, all_programs_in_database, all_ids_in_sheet):
        all_ids_in_database = [program.id for program in all_programs_in_database]

        programs_to_delete = [
            program_id
            for program_id in all_ids_in_database
            if program_id not in all_ids_in_sheet
        ]

        return programs_to_delete

    def remove_deleted_programs(self):
        """Delete programs from the database.

        This function deletes programs from the database, if those
        programs no longer reside in the Master Google Sheet. This may
        happen when a local Goodwill outright deletes a program from
        their local sheet.
        """
        # 1. Get all IDs in the Google sheet by filtering a Pandas dataframe.
        all_ids_in_sheet = self.dataframe_of_google_sheet[
            "Row Identifier (DO NOT EDIT)"
        ].tolist()

        with self.engine.connect() as connection:
            # 2. Get all IDs in the Pathways database
            select_all = select([self.programs_table])
            all_programs_in_database = connection.execute(select_all)

            # 3. Determine if the Pathways database has IDs that the Google Sheet does not have
            programs_to_delete = self._find_programs_to_delete(
                all_programs_in_database, all_ids_in_sheet
            )

            # 4. (OPTIONAL) Delete programs from Pathways database
            if programs_to_delete:
                delete_object = self.programs_table.delete().where(
                    self.programs_table.c.id.in_(
                        program for program in programs_to_delete
                    )
                )
                connection.execute(delete_object)

        return programs_to_delete

    def remove_programs_not_marked_for_pathways(self):
        """Delete programs from the database.

        This function deletes programs from the database, if those
        programs do not have the value 'Yes' in the field 'Should this
        program be available in Google Pathways?'. This may happen when
        a local Goodwill decides that a program should no longer appear
        in pathways, and they adjust this preference in their local
        sheet.
        """
        df = self.dataframe_of_google_sheet
        filtered_df = df[
            df["Should this program be available in Google Pathways?"] != "Yes"
        ]

        non_pathways_programs = filtered_df["Row Identifier (DO NOT EDIT)"].tolist()
        # It is possible that a local Goodwill will manually (and erroneously) create an entry, i.e., the program does programmatically get a Row Identifier.
        non_pathways_programs = [
            program for program in non_pathways_programs if program is not None
        ]

        if non_pathways_programs:
            with self.engine.connect() as connection:
                delete_object = self.programs_table.delete().where(
                    self.programs_table.c.id.in_(
                        program for program in non_pathways_programs
                    )
                )
                connection.execute(delete_object)

        return non_pathways_programs
