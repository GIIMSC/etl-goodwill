from sqlalchemy import MetaData, Table, create_engine

from config.config import (GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_IDS,
                           SQLALCHEMY_DATABASE_URI)
from etl.extractor import Extractor
from etl.loaders.loader import Loader
from etl.loaders.pathways_loader import PathwaysLoader
from etl.transformers.transformer import Transformer
from etl.transformers.pathways_transformer import PathwaysTransformer
from etl.utils.logger import logger

engine = create_engine(SQLALCHEMY_DATABASE_URI)


for goodwill, spreadsheet_id in SPREADSHEET_IDS.items():
    logger.info("----Running ETL for {}".format(goodwill))
    
    sheet_as_list = Extractor(
        google_account_info=GOOGLE_DRIVE_CREDENTIALS, 
        spreadsheet_id=spreadsheet_id
    ).get_sheet_as_list()
    
    if sheet_as_list:
        logger.info("----Data found")
        dataframe = Transformer(
            sheet=sheet_as_list, 
            spreadsheet_id=spreadsheet_id,
            engine=engine
        ).transform()
        
        pathways_dataframe = PathwaysTransformer(
            dataframe=dataframe
        ).pathways_transform()

        # Loader(
        #     dataframe=dataframe, 
        #     engine=engine
        # ).load_data()