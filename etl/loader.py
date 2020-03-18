from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects import postgresql

from etl.utils.logger import logger

class Loader:
    def __init__(self, dataframe, sqlalchemy_database_uri, spreadsheet_id):
        self.dataframe = dataframe
        self.engine = create_engine(sqlalchemy_database_uri)
        self.spreadsheet_id = spreadsheet_id

    def _filter_last_updated(self):
        query = '''
            SELECT "LastUpdated"
            FROM programs
            WHERE source_sheet_id = %s
            ORDER BY "LastUpdated" DESC
            LIMIT 1
        '''
        with self.engine.connect() as connection:
            results = connection.execute(query, self.spreadsheet_id)
            try:
                last_updated = results.fetchone()['LastUpdated']
            except TypeError:
                # An empty programs table return zero results, i.e., the first time running this script.
                return self.dataframe
            else:
                try:
                    self.dataframe['LastUpdated'] = pd.to_datetime(self.dataframe['LastUpdated'], format='%m/%d/%Y %H:%M:%S')
                except ValueError as e:
                    self.dataframe['LastUpdated'] = pd.to_datetime(self.dataframe['LastUpdated'], format='%Y-%m-%d %H:%M:%S')
                
                return self.dataframe[self.dataframe['LastUpdated'] > last_updated]

    def _intersect_columns(self):
        filtered_df = self._filter_last_updated()
        query = '''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'programs';
        '''
        with self.engine.connect() as connection:
            results = connection.execute(query)
            table_columns = [desc[0] for desc in results.fetchall()]
            
            return filtered_df[filtered_df.columns.intersection(table_columns)]
    
    def load_data(self):
        """
        This function uses `on_conflict_do_update` from `sqlalchemy.dialects.postgresql`, which
        runs a query against the programs table: it either INSERTS a new row, or it UPDATES
        existing rows.

        The query looks like this:
        ```
        INSERT INTO programs (gs_row_identifier...) 
        VALUES (%(gs_row_identifier)s...)
        ON CONFLICT (gs_row_identifier)
        DO UPDATE SET gs_row_identifier = %(param_1)s...
        ```
        """
        loadable_cols_df = self._intersect_columns()
        loadable_dict = loadable_cols_df.to_dict(orient='records')

        metadata = MetaData(bind=self.engine)
        programs_table = Table('programs', metadata, autoload=True)

        for row in loadable_dict:
            row = { field_name: None if not value else value for field_name, value in row.items() }

            with self.engine.connect() as connection:
                try:
                    sql_insert = postgresql.insert(programs_table).values(**row)
                    sql_upsert = sql_insert.on_conflict_do_update(
                        index_elements=['gs_row_identifier'],
                        set_=row)
                    
                    connection.execute(sql_upsert)
                except Exception as e:
                    logger.error(e)
                    continue
