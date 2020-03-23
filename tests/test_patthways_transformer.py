import pandas as pd

from etl.transformers.pathways_transformer import PathwaysTransformer


def test_make_prereq_blob(transformer):
    df = pd.DataFrame({
        "IsDiplomaRequired": ['Yes'],
        "EligibleGroups": ["Youth"],
        'MaxIncomeEligibility': ['20000'],
        'Prerequisites': 'Other'
    })

    transformer = PathwaysTransformer(dataframe=df)
    row = df.iloc[0]

    results = transformer._make_prereq_blob(row)

    pass