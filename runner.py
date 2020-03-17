from config.config import (GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_IDS,
                           SQLALCHEMY_DATABASE_URI)
from etl.extractor import Extractor
from etl.loader import Loader
from etl.transformer import Transformer

for goodwill, spreadsheet_id in SPREADSHEET_IDS.items():
    print("----Running ETL for {}".format(goodwill))
    
    sheet_as_list = Extractor(
        google_account_info=GOOGLE_DRIVE_CREDENTIALS, 
        spreadsheet_id=spreadsheet_id
    ).get_sheet_as_list()
    
    if sheet_as_list:
        print("----Data found")
        dataframe = Transformer(
            sheet=sheet_as_list, 
            spreadsheet_id=spreadsheet_id
        ).transform()
        
        # Loader(
        #     dataframe=dataframe, 
        #     sqlalchemy_database_uri=SQLALCHEMY_DATABASE_URI,
        #     spreadsheet_id=spreadsheet_id
        # ).load_data()