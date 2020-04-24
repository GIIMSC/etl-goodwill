import pandas as pd


def make_dataframe_with_headers(list_of_lists):
    """This function converts the list of lists into a Pandas dataframe with
    headers (and without a zeroeth row that contains header names)."""
    headers = list_of_lists[0]

    df = pd.DataFrame(list_of_lists, columns=headers)
    df.drop(df.index[0], inplace=True)

    return df
