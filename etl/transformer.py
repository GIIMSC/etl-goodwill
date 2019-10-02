import pandas as pd
import numpy as np

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
        "Mission Impact Program ID": "MissionImpactProgramId",
        "Program Status": "ProgramStatus",
        "CIP Code": "CIP",
        "Program Address (if different from organization address)": "ProgramAddress",
        "URL of Program": "ProgramUrl",
        "Contact phone number for program": "ContactPhone",
        "Program description": "ProgramDescription",
        "Should this program be available in Google Pathways?": "PathwaysEnabled",
        "Cost of program, in dollars": "ProgramFees",
        "Duration / Time to complete": "ProgramLength",
        "Total Units": "TotalUnits",
        "Unit Cost (not required if total cost is given)": "UnitCost",
        "Format": "Format",
        "Timing": "Timing",
        "Books, materials, supplies cost (in dollars)": "CostOfBooksAndSupplies",
        "Start date(s)": "StartDate",
        "End date(s)": "EndDate",
        "Credential level earned": "CredentialLevelEarned",
        "Accreditation body name": "AccreditationBodyName",
        "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?": "CredentialEarned",
        "What occupations/jobs does the training prepare you for?": "RelatedOccupations",
        "Apprenticeship or Paid Training Available": "IsPaid",
        "If yes, average hourly wage paid to student": "AverageWagePaid",
        "Incentives": "Incentives",
        "Salary post-graduation": "PostGradSalary",
        "Eligibile groups": "EligibleGroups",
        "Maximum yearly household income to be eligible": "MaxIncomeEligibility",
        "HS diploma required?": "IsDiplomaRequired",
        "Other prerequisites": "Prerequisites",
        "Anything else to add about the program?": "Miscellaneous",
        "Row Identifier (DO NOT EDIT)": "gs_row_identifier",
    }

    def __init__(self, sheet, spreadsheet_id):
        self.sheet = sheet
        self.spreadsheet_id = spreadsheet_id

    def _make_dataframe_with_headers(self):
        '''
        This function instantiates the Pandas dataframe with headers
        that map to the field names in the Data Resource API.

        Note! We can likely remove the renaming of the dataframe (via `header_mappings`), 
        if the Google sheet/script can be altered to match the schema.
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
        df_clean.drop(df_clean.index[0], inplace=True)

        return df_clean
    
    def transform(self):
        df = self._make_dataframe_with_headers()
        df_with_source = self._add_source_id(df)
        df_clean = self._clean_dataframe(df_with_source)

        return df_clean
