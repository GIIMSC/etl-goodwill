from datetime import datetime

import numpy as np
import pandas as pd


class DataframeTransformer:
    """Transforms Google sheet data (list of lists) into a Pandas dataframe
    with headers and date formatting, which has been filtered to include only
    recently updated data.

    Attributes:
        sheet: a list of lists (abstraction of Google sheet)
        engine: a sqlalchemy engine
    """

    def __init__(self, sheet, engine):
        self.sheet = sheet
        self.engine = engine

    def _make_dataframe_with_headers(self):
        """This function converts the list of lists into a Pandas dataframe
        with headers (and without a zeroeth row that contains header names)."""
        headers = self.sheet[0]

        df = pd.DataFrame(self.sheet, columns=headers)
        df.drop(df.index[0], inplace=True)

        return df

    def _format_date(self, date: str):
        return datetime.strptime(date.strip(), "%m/%d/%Y").date().isoformat()

    def _format_startdates_and_enddates(self, dates: str):
        """The Goodwill intake form allows multiple responses for "Start
        date(s)" and "End date(s)." Specifically, the form asks:

        "Format dates as mm/dd/yyyy or mm/dd.
        If more than one, please separate each value with a semi-colon (e.g., 01/21/2020; 07/19/2020; 10/05/2020)."

        This function splits the values into a list, and then converts each value to isoformat.
        """

        def format_and_catch_exception(date):
            try:
                formatted_date = self._format_date(date)
            except ValueError:
                formatted_date = date
            return formatted_date

        dates_as_list = dates.split(";")
        formatted_dates = [format_and_catch_exception(entry) for entry in dates_as_list]

        return formatted_dates

    def _format_date_or_invalid(self, date):
        try:
            formatted_date = self._format_date(date)
        except ValueError:
            formatted_date = self._format_date("09/09/2099")
        return formatted_date

    def _handle_dates(self, df):
        df["Application Deadline"] = df["Application Deadline"].apply(
            self._format_date_or_invalid
        )
        df["Start date(s)"] = df["Start date(s)"].apply(
            self._format_startdates_and_enddates
        )
        df["End Date(s)"] = df["End Date(s)"].apply(
            self._format_startdates_and_enddates
        )

        return df

    def _filter_last_updated(self, dataframe):
        query = """
            SELECT updated_at
            FROM pathways_program
            ORDER BY updated_at DESC
            LIMIT 1
        """
        with self.engine.connect() as connection:
            results = connection.execute(query)
            try:
                last_updated = results.fetchone()["updated_at"]
            except TypeError:
                # An empty programs table return zero results, i.e., the first time running this script.
                return dataframe
            else:
                try:
                    dataframe["Timestamp"] = pd.to_datetime(
                        dataframe["Timestamp"], format="%m/%d/%Y %H:%M:%S"
                    )
                except ValueError:
                    dataframe["Timestamp"] = pd.to_datetime(
                        dataframe["Timestamp"], format="%Y-%m-%d %H:%M:%S"
                    )

                return dataframe[dataframe["Timestamp"] > last_updated]

    def transform(self):
        df = self._make_dataframe_with_headers()
        df_with_valid_dates = self._handle_dates(df)
        clean_df = self._filter_last_updated(df_with_valid_dates)

        return clean_df
