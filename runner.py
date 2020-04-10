from sqlalchemy import MetaData, Table, create_engine

from config.config import (GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_IDS,
                           SQLALCHEMY_DATABASE_URI)
from etl.extractor import Extractor
from etl.loader import Loader
from etl.transformers.dataframe_transformer import DataframeTransformer
from etl.transformers.pathways_transformer import PathwaysTransformer
from etl.utils.logger import logger

engine = create_engine(SQLALCHEMY_DATABASE_URI)


for goodwill, spreadsheet_id in SPREADSHEET_IDS.items():
    logger.info('----Running ETL for {}'.format(goodwill))

    sheet_as_list = Extractor(
        google_account_info=GOOGLE_DRIVE_CREDENTIALS,
        spreadsheet_id=spreadsheet_id
    ).get_sheet_as_list()

    if sheet_as_list:
        logger.info('----Data found')
        dataframe = DataframeTransformer(
            sheet=sheet_as_list,
            engine=engine
        ).transform()

        pathways_dataframe = PathwaysTransformer(
            dataframe=dataframe
        ).pathways_transform()

        loader = Loader(engine=engine)
        loader.load_data(
            dataframe=pathways_dataframe,
            table_name='pathways_program',
            primary_key='id'
        )