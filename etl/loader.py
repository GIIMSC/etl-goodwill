from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine


class Loader:
    def __init__(self, dataframe, sqlalchemy_database_uri):
        self.dataframe = dataframe
        self.engine = create_engine(sqlalchemy_database_uri)

    def _filter_last_updated(self):
        # TODO: This should only consider the Goodwill being loaded:
        # Add a source_spreadsheet_id to the schema (but hide it!)
        # Pass that in as an attr on the loader class
        # Add a WHERE to this query
        query = '''
            SELECT "LastUpdated"
            FROM programs
            ORDER BY "LastUpdated" DESC
            LIMIT 1
        '''
        with self.engine.connect() as connection:
            results = connection.execute(query)
            try:
                last_updated = results.fetchone()['LastUpdated']
            except TypeError:
                # An empty programs table return zero results, i.e., the first time running this script.
                return self.dataframe
            else:
                self.dataframe['LastUpdated'] = pd.to_datetime(self.dataframe['LastUpdated'], format='%m/%d/%Y %H:%M:%S')
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
        loadable_cols_df = self._intersect_columns()
        loadable_cols_df.to_sql(
            'programs', 
            con=self.engine, 
            if_exists='append', 
            index=False)
