import pytest
from sqlalchemy.engine import Connection, Engine, ResultProxy, RowProxy


def test_make_dataframe_with_headers(transformer):
    df = transformer._make_dataframe_with_headers()
    headers_in_dataframe = df.keys()
    headers_in_mapping = [value for key, value in transformer.header_mappings.items()]

    for header in headers_in_mapping:
        assert header in headers_in_dataframe


def test_add_source_id(transformer):
    df = transformer._make_dataframe_with_headers()
    output_df = transformer._add_source_id(df)

    assert output_df["source_sheet_id"][1] == transformer.spreadsheet_id


def test_add_source_id(transformer):
    df = transformer._make_dataframe_with_headers()
    output_df = transformer._add_source_id(df)

    assert output_df["source_sheet_id"][1] == transformer.spreadsheet_id


def test_clean_dataframe(transformer):
    df = transformer._make_dataframe_with_headers()
    original_zeroeth_index = df.values[0]

    for header in transformer.header_mappings.keys():
        assert header in original_zeroeth_index

    output_df = transformer._clean_dataframe(df)
    new_zeroeth_index = output_df.values[0]

    for header in transformer.header_mappings.keys():
        assert header not in new_zeroeth_index


@pytest.mark.parametrize('input, expected_output', [
    ('12/15/2019', ['2019-12-15']),
    ('01/15/2019;12/15/2020', ['2019-01-15','2020-12-15']),
    ('01/15/2019; 12/15/2020', ['2019-01-15','2020-12-15']),
    ('01/15/2019; 12/15/2020; 7/01/2019', ['2019-01-15','2020-12-15', '2019-07-01']),
    ('Open Enrollment', ['Open Enrollment'])
])
def test_format_startdates_and_enddates(transformer, input, expected_output):
    formatted_dates = transformer._format_startdates_and_enddates(input)

    assert formatted_dates == expected_output


@pytest.mark.parametrize('input, expected_output', [
    ('12/15/2019', '2019-12-15'),
    ('09/09/9999', '9999-09-09'),
    ('', '9999-09-09'),
])
def test_format_date_or_invalid(transformer, input, expected_output):
    formatted_dates = transformer._format_date_or_invalid(input)

    assert formatted_dates == expected_output


def test_handle_dates(transformer):
    df = transformer._make_dataframe_with_headers()
    transformed_df = transformer._handle_dates(df)

    assert transformed_df['ApplicationDeadline'][1] == '2021-01-15'
    assert transformed_df['StartDates'][1] == ['2021-07-15']
    assert transformed_df['EndDates'][1] == ['2021-12-15']


@pytest.mark.parametrize('database_last_updated,evaluates_as', [
    ('03/19/2020 07:25:00', True),
    ('03/16/2020 07:25:00', False)
])
def test_filter_last_updated(mocker, transformer, database_last_updated, evaluates_as):
    conn = Connection(engine=transformer.engine)
    mocker.patch("sqlalchemy.create_engine", return_value=Engine)
    mocker.patch("sqlalchemy.engine.Engine.connect", return_value=conn)
    mocker.patch("sqlalchemy.engine.Engine.execute", return_value=ResultProxy)
    mocker.patch("sqlalchemy.engine.ResultProxy.fetchone", return_value={'LastUpdated': database_last_updated})

    df = transformer._make_dataframe_with_headers()
    df = transformer._clean_dataframe(df)
    full_df_count = df.shape[0]

    df = transformer._filter_last_updated(df)
    filtered_df_count = df.shape[0]

    assert (filtered_df_count < full_df_count) == evaluates_as
