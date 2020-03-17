import pytest

from etl.transformer import Transformer


@pytest.mark.parametrize('input, expected_output', [
    ('12/15/2019', ['2019-12-15']),
    ('01/15/2019;12/15/2020', ['2019-01-15','2020-12-15']),
    ('01/15/2019; 12/15/2020', ['2019-01-15','2020-12-15']),
    ('01/15/2019; 12/15/2020; 7/01/2019', ['2019-01-15','2020-12-15', '2019-07-01']),
    ('Open Enrollment', ['Open Enrollment'])
])
def test_format_startdates_and_enddates(input, expected_output):
    transformer = Transformer(sheet=[], spreadsheet_id='12345678')
    formatted_dates = transformer._format_startdates_and_enddates(input)

    assert formatted_dates == expected_output