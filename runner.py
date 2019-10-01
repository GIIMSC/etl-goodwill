from config.config import (GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_IDS,
                           SQLALCHEMY_DATABASE_URI)
from etl.extractor import Extractor
from etl.loader import Loader
from etl.transformer import Transformer

for goodwill, spreadsheet_id in SPREADSHEET_IDS.items():
    print("----Running ETL for {}".format(goodwill))
    sheet_as_list = Extractor(GOOGLE_DRIVE_CREDENTIALS, spreadsheet_id).get_sheet_as_list()
    dataframe = Transformer(sheet_as_list).clean_dataframe()
    Loader(dataframe, SQLALCHEMY_DATABASE_URI).load_data()