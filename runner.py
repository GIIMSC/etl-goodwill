from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.loader import Loader

from config.config import GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID, SQLALCHEMY_DATABASE_URI

sheet_as_list = Extractor(GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID).get_sheet_as_list()
dataframe = Transformer(sheet_as_list).clean_dataframe()
Loader(dataframe, SQLALCHEMY_DATABASE_URI).load_data()
