from datetime import datetime

import numpy as np
import pandas as pd


class Transformer:
    header_mappings = {
        "Timestamp": "LastUpdated",
        "Organization Name": "ProgramProvider",
        "Organization URL": "ProviderUrl",
        "Organization Address": "ProviderAddress",
        "Program Name": "ProgramName",
        "Program Category": "ProgramCategory", 
        "Population(s) Targeted": "PopulationTargeted",
        "Goal/Outcome": "Goal",
        "Time Investment": "TimeInvestment",
        "Program ID": "ProgramId",
        "Program Status": "ProgramStatus",
        "CIP Code": "CIP",
        "Application Deadline": "ApplicationDeadline", 
        "Program Address (if different from organization address)": "ProgramAddress",
        "URL of Program": "ProgramUrl",
        "Contact phone number for program": "ContactPhone",
        "Program description": "ProgramDescription",
        "Should this program be available in Google Pathways?": "PathwaysEnabled",
        "Total cost of the program (in dollars)": "TotalCostOfProgram",
        "Duration / Time to complete": "ProgramLength",
        "Total Units": "TotalUnits",
        "Unit Cost (not required if total cost is given)": "UnitCost",
        "Format": "Format",
        "Timing": "Timing",
        "Start date(s)": "StartDates",
        "End Date(s)": "EndDates",
        "Credential level earned": "CredentialLevelEarned",
        "Accreditation body name": "AccreditationBodyName",
        "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?": "CredentialEarned",
        "What occupations/jobs does the training prepare you for?": "RelatedOccupations",
        "Apprenticeship or Paid Training Available": "IsPaid",
        "If yes, average hourly wage paid to student": "AverageHourlyWagePaid",
        "Incentives": "Incentives",
        "Average ANNUAL salary post-graduation": "PostGradAnnualSalary",
        "Average HOURLY wage post-graduation": "PostGradHourlyWage",
        "Eligible groups": "EligibleGroups",
        "Maximum yearly household income to be eligible": "MaxIncomeEligibility",
        "HS diploma required?": "IsDiplomaRequired",
        "Other prerequisites": "Prerequisites",
        "Anything else to add about the program?": "Miscellaneous",
        "Maximum Enrollment": "MaximumEnrollment",
        "Row Identifier (DO NOT EDIT)": "gs_row_identifier",
    }

    def __init__(self, sheet, spreadsheet_id):
        self.sheet = sheet
        self.spreadsheet_id = spreadsheet_id

    def _make_dataframe_with_headers(self):
        '''
        This function instantiates the Pandas dataframe with headers
        that map to the field names in the Data Resource API.
        '''
        headers = self.sheet[0]
        dataframe_obj = pd.DataFrame(self.sheet, columns=headers)

        return dataframe_obj.rename(index=str, columns=self.header_mappings)
    
    def _add_source_id(self, df):
        df["source_sheet_id"] = self.spreadsheet_id

        return df

    def _clean_dataframe(self, df):
        '''
        This function removes the zeroeth row, which contains header names, from the dataframe.
        '''
        df.drop(df.index[0], inplace=True)

        return df
    
    def _format_date(self, date: str):
        return datetime.strptime(date.strip(), '%m/%d/%Y').date().isoformat()

    def _format_startdates_and_enddates(self, dates: str):
        '''
        The Goodwill intake form allows multiple responses for "Start date(s)" and "End date(s)." 
        Specifically, the form asks: 

        "Format dates as mm/dd/yyyy or mm/dd. 
        If more than one, please separate each value with a semi-colon (e.g., 01/21/2020; 07/19/2020; 10/05/2020)."

        This function splits the values into a list, and then converts each value to isoformat.
        '''
        def format_and_catch_exception(date):
            try:
                formatted_date = self._format_date(date)
            except ValueError:
                formatted_date = date
            return formatted_date

        dates_as_list = dates.split(';')
        formatted_dates = [format_and_catch_exception(entry) for entry in dates_as_list]

        return formatted_dates

    def _format_date_or_invalid(self, date):
        try:
            formatted_date = self._format_date(date)
        except ValueError:
            formatted_date = self._format_date("09/09/9999")
        return formatted_date

    def _handle_dates(self, df): 
        df["ApplicationDeadline"] = df["ApplicationDeadline"].apply(self._format_date_or_invalid)
        df["StartDates"] = df["StartDates"].apply(self._format_startdates_and_enddates)
        df["EndDates"] = df["EndDates"].apply(self._format_startdates_and_enddates)
        
        return df
    
    def transform(self):
        df = self._make_dataframe_with_headers()
        df_with_source = self._add_source_id(df)
        df_clean = self._clean_dataframe(df_with_source)
        df_with_valid_dates = self._handle_dates(df_clean)

        return df_with_valid_dates
