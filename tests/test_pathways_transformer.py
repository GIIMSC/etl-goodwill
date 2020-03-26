import pandas as pd
import pytest

from etl.transformers.pathways_transformer import PathwaysTransformer
from etl.utils.errors import InvalidPathwaysData


def test_make_prereq_blob_all():
    df = pd.DataFrame({
        'IsDiplomaRequired': ['Yes'],
        'EligibleGroups': ['Veteran'],
        'MaxIncomeEligibility': ['40000'],
        'Prerequisites': ['Reliable transportation']
    })
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    assert results['credential_category'] == 'HighSchool'
    assert results['eligible_groups'] == 'Veteran'
    assert results['max_income_eligibility'] == '40000'
    assert results['other_program_prerequisites'] == 'Reliable transportation'


def test_make_prereq_blob_one():
    df = pd.DataFrame({
        'IsDiplomaRequired': ['Yes'],
        'EligibleGroups': [''],
        'MaxIncomeEligibility': [''],
        'Prerequisites': ['']
    })
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    assert results['credential_category'] == 'HighSchool'
    assert 'eligible_groups' not in results
    assert 'max_income_eligibility' not in results
    assert 'other_program_prerequisites' not in results


def test_make_address_blob_for_provider_and_program():
    df = pd.DataFrame({
        'ProviderAddress': ['111 Grickle Grass Lane, Springfield MA 88884'],
        'ProgramAddress': ['234 Mulberry Street, Chicago IL 74444']
    })
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_address_blob(row)
    expected_results = [{'street_address': '111 Grickle Grass Lane', 'address_locality': 'Springfield', 'address_region': 'MA', 'postal_code': '88884'}, {'street_address': '234 Mulberry Street', 'address_locality': 'Chicago', 'address_region': 'IL', 'postal_code': '74444'}]
    
    assert len(results) == 2
    assert expected_results == results


def test_make_address_blob_for_provider():
    df = pd.DataFrame({
        'ProviderAddress': ['111 Grickle Grass Lane, Springfield MA 88884'],
        'ProgramAddress': ['']
    })
    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_address_blob(row)
    expected_results = [{'street_address': '111 Grickle Grass Lane', 'address_locality': 'Springfield', 'address_region': 'MA', 'postal_code': '88884'}]
    
    assert len(results) == 1
    assert expected_results == results


@pytest.mark.parametrize('input,expected_output', [
    ('14 days', 'P14D'),
    ('5 weeks', 'P5W'),
    ('6 months', 'P6M')
])
def test_convert_duration_to_isoformat(input, expected_output):
    transformer = PathwaysTransformer(dataframe=pd.DataFrame({}))
    output = transformer._convert_duration_to_isoformat(input)

    assert output == expected_output


@pytest.mark.parametrize('input', [
    ('Full Time Permanent Employment'),
    ('1 week'),
    ('')
])
def test_convert_duration_to_isoformat_error(input):
    transformer = PathwaysTransformer(dataframe=pd.DataFrame({}))

    with pytest.raises(InvalidPathwaysData) as exceptionMsg:
        transformer._convert_duration_to_isoformat(input)

        assert 'The program does not have parseable input in "Duration / Time to complete"' in str(exceptionMsg.value)