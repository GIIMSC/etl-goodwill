import re

import pandas as pd
import usaddress

from converter import (educational_occupational_programs_converter,
                       work_based_programs_converter)
from etl.transformers.transformer import Transformer
from etl.utils.errors import InvalidPathwaysData
from etl.utils.logger import logger


class PathwaysTransformer(Transformer):
    def __init__(self, dataframe):
        self.dataframe = dataframe

  
    def _make_prereq_blob(self, row):
        prereq_blob = {}

        if getattr(row, 'IsDiplomaRequired'):
            prereq_blob['credential_category'] = 'HighSchool'
        
        if getattr(row, 'EligibleGroups'):
            prereq_blob['eligible_groups'] = getattr(row, 'EligibleGroups')
        
        if getattr(row, 'MaxIncomeEligibility'):
            prereq_blob['max_income_eligibility'] = getattr(row, 'MaxIncomeEligibility')

        if getattr(row, 'Prerequisites'):
            prereq_blob['other_program_prerequisites'] = getattr(row, 'Prerequisites')
        
        return prereq_blob


    def _make_address_blob(self, row):
        def parse_address(address):
            parsed_address = usaddress.tag(address)[0]
            street_address = f"{parsed_address.get('AddressNumber', '')} {parsed_address.get('StreetNamePreDirectional', '')} {parsed_address.get('StreetName', '')} {parsed_address.get('StreetNamePostType', '')}"
            street_address = re.sub(' +', ' ', street_address) # Replace double spaces with single space

            return {
                'street_address': street_address, 
                'address_locality': parsed_address.get('PlaceName'), 
                'address_region': parsed_address.get('StateName'), 
                'postal_code': parsed_address.get('ZipCode')
            }

        provider_address_list = []
        provider_address = getattr(row, 'ProviderAddress')
        program_address = getattr(row, 'ProgramAddress')
        
        try:
            parsed_provider_address = parse_address(provider_address)
            provider_address_list.append(parsed_provider_address)
        except Exception as err:
            logger.error(err)

        if program_address:
            try:
                parsed_program_address = parse_address(program_address)
                provider_address_list.append(parsed_program_address)
            except Exception as err:
                logger.error(err)
        
        return provider_address_list


    def _convert_duration_to_isoformat(self, duration):
        duration_count = duration.split(' ')[0]

        if 'days' in duration:
            duration_in_isoformat = f'P{duration_count}D'
        elif 'weeks' in duration:
            duration_in_isoformat = f'P{duration_count}W'
        elif 'months' in duration:
            duration_in_isoformat = f'P{duration_count}M'
        else:
            raise InvalidPathwaysData('The program does not have parseable input in "Duration / Time to complete" – needed for "TimeToComplete."')
        
        return duration_in_isoformat
    

    def _convert_to_pathways_json(self):
        for row in self.dataframe.itertuples(index=True, name='Pandas'):
            gs_row_identifier = getattr(row, 'gs_row_identifier')
            provider_address = self._make_address_blob(row)

            try:
                time_to_complete = self._convert_duration_to_isoformat(getattr(row, 'ProgramLength'))
            except InvalidPathwaysData as err:
                logger.error(f"Could not transform data. Google Sheet Row: {gs_row_identifier}")
                logger.error(err)
                continue

            if getattr(row, 'IsPaid') == 'Yes':
                input_kwargs = {
                    'program_description': getattr(row, 'ProgramDescription'),
                    'program_name': getattr(row, 'ProgramName'),
                    'program_url': getattr(row, 'ProgramUrl'), 
                    'provider_name': getattr(row, 'ProgramProvider'), 
                    'provider_url': getattr(row, 'ProviderUrl'), 
                    'provider_telephone': getattr(row, 'ContactPhone'), 
                    'provider_address': provider_address, 
                    'start_date': getattr(row, 'StartDates'), # I think this is a list?
                    'end_date': getattr(row, 'EndDates'), # I think this is a list?
                    'maximum_enrollment': getattr(row, 'MaximumEnrollment'), 
                    'occupational_credential_awarded': getattr(row, 'CredentialEarned'), 
                    'time_of_day': getattr(row, 'Timing'),
                    'time_to_complete': time_to_complete, 
                    'offers_price': getattr(row, 'TotalCostOfProgram'),
                    'training_salary': getattr(row, 'AverageHourlyWagePaid'),
                    'salary_upon_completion': getattr(row, 'PostGradAnnualSalary')
                }

                prereq_blob = self._make_prereq_blob(row)
                if prereq_blob:
                    input_kwargs['program_prerequisites'] = prereq_blob

                pathways_json_ld = work_based_programs_converter(**input_kwargs)
            else:
                input_kwargs = {
                    'application_deadline': getattr(row, 'ApplicationDeadline'), 
                    'program_name': getattr(row, 'ProgramName'), 
                    'program_description': getattr(row, 'ProgramDescription'),
                    'offers_price': getattr(row, 'TotalCostOfProgram'), 
                    'program_url': getattr(row, 'ProgramUrl'), 
                    'provider_name': getattr(row, 'ProgramProvider'), 
                    'provider_url': getattr(row, 'ProviderUrl'), 
                    'provider_telephone': getattr(row, 'ContactPhone'), 
                    'provider_address': provider_address, 
                    'time_to_complete': '', 
                    'identifier_cip': getattr(row, 'CIP'), 
                    'identifier_program_id': getattr(row, 'ProgramId'),   
                    'start_date': getattr(row, 'StartDates'), # I think this is a list?
                    'end_date': getattr(row, 'EndDates'), # I think this is a list?
                    'occupational_credential_awarded': getattr(row, 'CredentialEarned'), 
                    'maximum_enrollment': getattr(row, 'MaximumEnrollment'), 
                    'time_of_day': getattr(row, 'Timing')
                }

                prereq_blob = self._make_prereq_blob(row)
                if prereq_blob:
                    input_kwargs['program_prerequisites'] = prereq_blob

                pathways_json_ld = educational_occupational_programs_converter(**input_kwargs)

            yield [gs_row_identifier, getattr(row, 'LastUpdated'), pathways_json_ld]
    
    def pathways_transform(self):
        '''
        This function consolidates the programs data into a "list of lists" (i.e., the same
        data type the Google API resource returns). Thus, we can 
        easily make use of the functions in the Transformer parent class.
        '''
        list_of_lists = []

        for list_with_pathways_program in self._convert_to_pathways_json():
            list_of_lists.append(list_with_pathways_program)
        
        headers = ['id', 'updated_at', 'pathways_program']
        dataframe_obj = pd.DataFrame(list_of_lists, columns=headers)
        
        return dataframe_obj
