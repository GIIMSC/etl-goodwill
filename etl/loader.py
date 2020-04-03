from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql

from etl.utils.logger import logger

class Loader:
    def __init__(self, engine):
        self.engine = engine

    
    def load_data(self, dataframe, table_name, primary_key):
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
        loadable_dict = dataframe.to_dict(orient='records')

        metadata = MetaData(bind=self.engine)
        programs_table = Table(table_name, metadata, autoload=True)

        for row in loadable_dict:
            row = { field_name: None if not value else value for field_name, value in row.items() }

            with self.engine.connect() as connection:
                try:
                    sql_insert = postgresql.insert(programs_table).values(**row)
                    sql_upsert = sql_insert.on_conflict_do_update(
                        index_elements=[primary_key],
                        set_=row)
                    
                    connection.execute(sql_upsert)
                except Exception as e:
                    logger.error(e)
                    continue
