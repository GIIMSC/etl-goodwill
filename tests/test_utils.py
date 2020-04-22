from etl.utils.utils import make_dataframe_with_headers


def test_make_dataframe_with_headers(google_sheet_data):
    df = make_dataframe_with_headers(google_sheet_data)
    headers_in_dataframe = df.keys()

    for header in headers_in_dataframe:
        assert header in google_sheet_data[0]
