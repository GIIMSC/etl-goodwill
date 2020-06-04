import pandas as pd
import pytest

from etl.transformers.pathways_transformer import PathwaysTransformer
from etl.utils.errors import InvalidPathwaysData


def test_filter_non_pathways_programs():
    df = pd.DataFrame(
        {
            "Program Name": ["Technical Academy Program", "Sales Leadership"],
            "Should this program be available in Google Pathways?": ["Yes", "No"],
        }
    )
    transformer = PathwaysTransformer(dataframe=df)

    filtered_df = transformer._filter_non_pathways_programs()

    assert len(transformer.dataframe) == 2
    assert len(filtered_df) == 1


def test_make_prereq_blob_all():
    df = pd.DataFrame({"HS diploma required?": ["Yes"], "Eligible groups": ["Veteran"]})
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    assert results["credential_category"] == "HighSchool"
    assert (
        results["competency_required"]
        == "Must belong to one or more of the following group(s): Veteran"
    )


def test_make_prereq_blob_credential():
    df = pd.DataFrame({"HS diploma required?": ["Yes"], "Eligible groups": [""]})
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    assert results["credential_category"] == "HighSchool"
    assert "competency_required" not in results


def test_make_prereq_blob_competency():
    df = pd.DataFrame({"HS diploma required?": ["No"], "Eligible groups": ["Veteran"]})
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    assert (
        results["competency_required"]
        == "Must belong to one or more of the following group(s): Veteran"
    )
    assert "credential_category" not in results


def test_make_address_blob_for_provider_and_program():
    df = pd.DataFrame(
        {
            "Organization Address": ["111 Grickle Grass Lane, Springfield MA 88884"],
            "Program Address (if different from organization address)": [
                "234 Mulberry Street, Chicago IL 74444"
            ],
        }
    )
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_address_blob(row)
    expected_results = [
        {
            "street_address": "111 Grickle Grass Lane",
            "address_locality": "Springfield",
            "address_region": "MA",
            "address_country": "US",
            "postal_code": "88884",
        },
        {
            "street_address": "234 Mulberry Street",
            "address_locality": "Chicago",
            "address_region": "IL",
            "address_country": "US",
            "postal_code": "74444",
        },
    ]

    assert len(results) == 2
    assert expected_results == results


def test_make_address_blob_for_provider():
    df = pd.DataFrame(
        {
            "Organization Address": ["111 Grickle Grass Lane, Springfield MA 88884"],
            "Program Address (if different from organization address)": [""],
        }
    )
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_address_blob(row)
    expected_results = [
        {
            "street_address": "111 Grickle Grass Lane",
            "address_locality": "Springfield",
            "address_region": "MA",
            "address_country": "US",
            "postal_code": "88884",
        }
    ]

    assert len(results) == 1
    assert expected_results == results


def test_make_address_blob_for_multiple_addresses_with_pipe():
    df = pd.DataFrame(
        {
            "Organization Address": ["234 Mulberry Street, Chicago IL 74444"],
            "Program Address (if different from organization address)": [
                "111 Grickle Grass Lane, Springfield MA 88884 | 3344 Lifted Lorax Street, Chicago IL 88884"
            ],
        }
    )
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_address_blob(row)
    expected_results = [
        {
            "street_address": "234 Mulberry Street",
            "address_locality": "Chicago",
            "address_region": "IL",
            "address_country": "US",
            "postal_code": "74444",
        },
        {
            "street_address": "111 Grickle Grass Lane",
            "address_locality": "Springfield",
            "address_region": "MA",
            "address_country": "US",
            "postal_code": "88884",
        },
        {
            "street_address": "3344 Lifted Lorax Street",
            "address_locality": "Chicago",
            "address_region": "IL",
            "address_country": "US",
            "postal_code": "88884",
        },
    ]

    assert len(results) == 3
    assert expected_results == results


@pytest.mark.parametrize(
    "input,expected_output",
    [("14 days", "P14D"), ("5 weeks", "P5W"), ("6 months", "P6M")],
)
def test_convert_duration_to_isoformat(input, expected_output):
    transformer = PathwaysTransformer(dataframe=pd.DataFrame({}))
    output = transformer._convert_duration_to_isoformat(input)

    assert output == expected_output


@pytest.mark.parametrize(
    "input", [("Full Time Permanent Employment"), ("1 week"), ("")]
)
def test_convert_duration_to_isoformat_error(input):
    transformer = PathwaysTransformer(dataframe=pd.DataFrame({}))

    with pytest.raises(InvalidPathwaysData) as exceptionMsg:
        transformer._convert_duration_to_isoformat(input)

        assert (
            'The program does not have parseable input in "Duration / Time to complete"'
            in str(exceptionMsg.value)
        )
