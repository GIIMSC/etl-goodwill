from etl.extractor import Extractor

from config import GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID

sheet_as_list = Extractor(GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID).get_sheet_as_list()
