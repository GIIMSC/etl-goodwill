# INPUT: a list of values
# for program in list_of_programs:
# 


# 1) Make transfromer into a Parent class.
# 2) Make a PathwaysTransformer
# 3) Make a 
from converter import educational_occupational_programs_converter, work_based_programs_converter

from etl.transformers.transformer import Transformer


class PathwaysTransformer(Transformer):
    def __init__(self, dataframe):
        self.dataframe = dataframe
    
    def transform_into_pathways_json(self):
        for row in self.dataframe.itertuples(index=True, name='Pandas'):
            # TODO: Something to parse the address
            # TODO: How are we going to handle ProgramLength?
            # TODO: Program Prerequisites blob, since some of it is conditional/optional

            input_kwargs = {
                'application_deadline': getattr(row, "ApplicationDeadline"),, 
                'program_name': getattr(row, "ProgramName"), 
                'offers_price': getattr(row, "TotalCostOfProgram"), 
                'program_url': getattr(row, "ProgramURL"), 
                'provider_name': getattr(row, "ProgramProvider"), 
                'provider_url': getattr(row, "ProviderUrl"), 
                'provider_telephone': getattr(row, "ContactPhone"), 
                'provider_address': [
                    {
                        'street_address': '1 Grickle Grass Lane', 
                        'address_locality': 'Springfield', 
                        'address_region': 'MA', 
                        'postal_code': '88881'
                    }
                ], 
                'time_to_complete': '', 
                'identifier_cip': getattr(row, "CIP"), 
                'identifier_program_id': getattr(row, "ProgramId"),  
                # 'program_prerequisites': {
                #     # 'credential_category': getattr(row, "IsDiplomaRequired") ----> if yes, then add this, 
                #     'eligible_groups': getattr(row, "EligibleGroups"), 
                #     'max_income_eligibility': getattr(row, "MaxIncomeEligibility"), 
                #     'other_program_prerequisites': getattr(row, "Prerequisites")
                # }, 
                'end_date': getattr(row, "EndDates"), # I think this is a list?
                'occupational_credential_awarded': getattr(row, "CredentialEarned"), 
                'maximum_enrollment': getattr(row, "MaximumEnrollment"), 
                'start_date': getattr(row, "StartDates"), 
                'time_of_day': getattr(row, "Timing")
            }
