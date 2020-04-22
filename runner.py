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

engine = create_engine(SQLALCHEMY_DATABASE_URI)

logger.info("----Running ETL")

sheet_as_list = Extractor(
    google_account_info=GOOGLE_DRIVE_CREDENTIALS, spreadsheet_id=MASTER_SHEET_ID
).get_sheet_as_list()

if sheet_as_list:
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
