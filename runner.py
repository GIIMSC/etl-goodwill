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
from etl.pathways_opt_out import OptOut


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
    opt_out = OptOut(
        google_sheet_as_list=sheet_as_list, programs_table=programs_table, engine=engine
    )
    deleted_records = opt_out.remove_deleted_programs()
    logger.info(
        f"----Deleted {len(deleted_records)} records from the Pathways database. {' '.join(deleted_records)}"
    )
    opt_out_records = opt_out.remove_programs_not_marked_for_pathways()

    logger.info(
        f"----Found {len(opt_out_records)} records that 'opt-out' of Pathways. {' '.join(opt_out_records)}"
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
        dataframe=pathways_dataframe, metadata_table=programs_table, primary_key="id"
    )
    logger.info("----Data loaded into Google Pathways API")
