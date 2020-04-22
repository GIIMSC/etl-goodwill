from sqlalchemy import MetaData, Table, create_engine

from config.config import (
    GOOGLE_DRIVE_CREDENTIALS,
    MASTER_SHEET_ID,
    SQLALCHEMY_DATABASE_URI,
)
from etl.extractor import Extractor
from etl.loader import Loader
from etl.transformers.dataframe_transformer import DataframeTransformer
from etl.transformers.pathways_transformer import PathwaysTransformer
from etl.utils.logger import logger
from etl.pathways_opt_out import (
    remove_deleted_programs,
    remove_programs_not_marked_for_pathways,
)


"""This ETL process accesses the Pathways database via a SQLAlchemy MetaData object, which describes the
database schema and this makes available a Table object.

Read more about database reflection: https://docs.sqlalchemy.org/en/13/core/reflection.html
"""
engine = create_engine(SQLALCHEMY_DATABASE_URI)
metadata = MetaData(bind=engine)
programs_table = Table("pathways_program", metadata, autoload=True)

logger.info("----Running ETL")

sheet_as_list = Extractor(
    google_account_info=GOOGLE_DRIVE_CREDENTIALS, spreadsheet_id=MASTER_SHEET_ID
).get_sheet_as_list()

if sheet_as_list:

    deleted_records = remove_deleted_programs(
        list_of_lists=sheet_as_list, programs_table=programs_table, engine=engine
    )
    logger.info(
        f"----Deleted {len(deleted_records)} records from the Pathways database. {' '.join(deleted_records)}"
    )

    dataframe = DataframeTransformer(sheet=sheet_as_list, engine=engine).transform()
    logger.info(
        f"----Initial transformation complete: {len(dataframe)} records prepped for PathwaysTransformer."
    )

    pathways_dataframe = PathwaysTransformer(dataframe=dataframe).pathways_transform()
    logger.info(
        f"----Pathways transformation complete: {len(pathways_dataframe)} records prepped for loading."
    )

    loader = Loader(engine=engine)
    loader.load_data(
        dataframe=pathways_dataframe, table_name="pathways_program", primary_key="id"
    )
    logger.info("----Data loaded into Google Pathways API")
