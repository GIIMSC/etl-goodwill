import pytest
from sqlalchemy.engine import Connection, Engine, ResultProxy, RowProxy


def test_make_dataframe_with_headers(google_sheet_data, transformer):
    df = transformer._make_dataframe_with_headers()
    headers_in_dataframe = df.keys()

    for header in headers_in_dataframe:
        assert header in google_sheet_data[0]


@pytest.mark.parametrize(
    "input, expected_output",
    [
        ("12/15/2019", ["2019-12-15"]),
        ("01/15/2019;12/15/2020", ["2019-01-15", "2020-12-15"]),
        ("01/15/2019; 12/15/2020", ["2019-01-15", "2020-12-15"]),
        (
            "01/15/2019; 12/15/2020; 7/01/2019",
            ["2019-01-15", "2020-12-15", "2019-07-01"],
        ),
        ("Open Enrollment", ["Open Enrollment"]),
    ],
)
def test_format_startdates_and_enddates(transformer, input, expected_output):
    formatted_dates = transformer._format_startdates_and_enddates(input)

    assert formatted_dates == expected_output


@pytest.mark.parametrize(
    "input, expected_output",
    [("12/15/2019", "2019-12-15"), ("09/09/2099", "2099-09-09"), ("", "2099-09-09")],
)
def test_format_date_or_invalid(transformer, input, expected_output):
    formatted_dates = transformer._format_date_or_invalid(input)

    assert formatted_dates == expected_output


def test_handle_dates(transformer):
    df = transformer._make_dataframe_with_headers()
    transformed_df = transformer._handle_dates(df)

    assert transformed_df["Application Deadline"][1] == "2021-01-15"
    assert transformed_df["Start date(s)"][1] == ["2021-07-15"]
    assert transformed_df["End Date(s)"][1] == ["2021-12-15"]


@pytest.mark.parametrize(
    "database_last_updated,evaluates_as",
    [("03/19/2020 07:25:00", True), ("03/16/2020 07:25:00", False)],
)
def test_filter_last_updated(
    mocker, connection_mock, transformer, database_last_updated, evaluates_as
):
    """How does this test work?

    The `google_sheet_data` fixture has "03/18/2020 07:25:37" as the
    updated_at value. This test mocks the return value of
    `results.fetchone()` with a value that is either higher or lower
    than the "03/18/2020 07:25:37" (updated_at).
    """
    mocker.patch("sqlalchemy.create_engine", return_value=Engine)
    mocker.patch("sqlalchemy.engine.Engine.connect", return_value=connection_mock)
    mocker.patch("sqlalchemy.engine.Engine.execute", return_value=ResultProxy)
    mocker.patch(
        "sqlalchemy.engine.ResultProxy.fetchone",
        return_value={"updated_at": database_last_updated},
    )

    df = transformer._make_dataframe_with_headers()
    full_df_count = df.shape[0]

    df = transformer._filter_last_updated(df)
    filtered_df_count = df.shape[0]

    assert (filtered_df_count < full_df_count) == evaluates_as
