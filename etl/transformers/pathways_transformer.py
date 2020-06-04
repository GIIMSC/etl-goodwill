import re

import pandas as pd
import usaddress

from converter import (
    educational_occupational_programs_converter,
    work_based_programs_converter,
)
from etl.utils.errors import InvalidPathwaysData
from etl.utils.logger import logger


class PathwaysTransformer:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def _make_prereq_blob(self, row):
        prereq_blob = {}

        if getattr(row, "HS diploma required?") == "Yes":
            prereq_blob["credential_category"] = "HighSchool"

        if getattr(row, "Eligible groups"):
            competency_description = f"Must belong to one or more of the following group(s): {getattr(row, 'Eligible groups')}"
            prereq_blob["competency_required"] = competency_description

        return prereq_blob

    def _make_address_blob(self, row):
        def parse_address(address):
            parsed_address = usaddress.tag(address)[0]
            street_address = f"{parsed_address.get('AddressNumber', '')} {parsed_address.get('StreetNamePreDirectional', '')} {parsed_address.get('StreetName', '')} {parsed_address.get('StreetNamePostType', '')}"
            street_address = re.sub(
                " +", " ", street_address
            )  # Replace double spaces with single space

            if street_address and street_address is not " ":
                return {
                    "street_address": street_address,
                    "address_locality": parsed_address.get("PlaceName"),
                    "address_region": parsed_address.get("StateName"),
                    "postal_code": parsed_address.get("ZipCode"),
                    "address_country": "US",
                }

        provider_address_list = []
        provider_address = getattr(row, "Organization Address")
        program_address = getattr(
            row, "Program Address (if different from organization address)"
        )

        try:
            parsed_provider_address = parse_address(provider_address)
            if parsed_provider_address:
                provider_address_list.append(parsed_provider_address)
        except Exception as err:
            logger.error(err)

        if program_address:
            program_address_as_list = program_address.split("|")

            for addr in program_address_as_list:
                try:
                    parsed_program_address = parse_address(addr)
                    if parsed_program_address:
                        provider_address_list.append(parsed_program_address)
                except Exception as err:
                    logger.error(err)

        return provider_address_list

    def _convert_duration_to_isoformat(self, duration):
        duration_count = duration.split(" ")[0]

        if "days" in duration:
            duration_in_isoformat = f"P{duration_count}D"
        elif "weeks" in duration:
            duration_in_isoformat = f"P{duration_count}W"
        elif "months" in duration:
            duration_in_isoformat = f"P{duration_count}M"
        else:
            raise InvalidPathwaysData(
                'The program does not have parseable input in "Duration / Time to complete" – needed for "TimeToComplete."'
            )

        return duration_in_isoformat

    def _filter_non_pathways_programs(self):
        return self.dataframe[
            self.dataframe["Should this program be available in Google Pathways?"]
            == "Yes"
        ]

    def _convert_to_pathways_json(self):
        df = self._filter_non_pathways_programs()

        for index, row in df.iterrows():
            gs_row_identifier = getattr(row, "Row Identifier (DO NOT EDIT)")
            provider_address = self._make_address_blob(row)

            try:
                time_to_complete = self._convert_duration_to_isoformat(
                    getattr(row, "Duration / Time to complete")
                )
            except InvalidPathwaysData as err:
                logger.error(
                    f"Could not transform data for Google Sheet Row: {gs_row_identifier}. See below error."
                )
                logger.error(err)
                continue

            if getattr(row, "Apprenticeship or Paid Training Available") == "Yes":
                input_kwargs = {
                    "program_description": getattr(row, "Program description"),
                    "program_name": getattr(row, "Program Name"),
                    "program_url": getattr(row, "URL of Program"),
                    "provider_name": getattr(row, "Goodwill Member Name"),
                    "provider_url": getattr(row, "Organization URL"),
                    "provider_telephone": getattr(
                        row, "Contact phone number for program"
                    ),
                    "provider_address": provider_address,
                    "start_date": getattr(
                        row, "Start date(s)"
                    ),  # I think this is a list?
                    "end_date": getattr(row, "End Date(s)"),  # I think this is a list?
                    "maximum_enrollment": getattr(row, "Maximum Enrollment"),
                    "occupational_credential_awarded": getattr(
                        row,
                        "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?",
                    ),
                    "time_of_day": getattr(row, "Timing"),
                    "time_to_complete": time_to_complete,
                    "offers_price": getattr(
                        row, "Total cost of the program (in dollars)"
                    ),
                    "training_salary": getattr(
                        row, "If yes, average hourly wage paid to student"
                    ),
                    "salary_upon_completion": getattr(
                        row, "Average ANNUAL salary post-graduation"
                    ),
                }

                prereq_blob = self._make_prereq_blob(row)
                if prereq_blob:
                    input_kwargs["program_prerequisites"] = prereq_blob

                try:
                    pathways_json_ld = work_based_programs_converter(**input_kwargs)
                except ValueError as err:
                    logger.error(
                        f"Could not transform data for Google Sheet Row: {gs_row_identifier}. See below error."
                    )
                    logger.error(err)
                    continue

            else:
                input_kwargs = {
                    "application_deadline": getattr(row, "Application Deadline"),
                    "program_name": getattr(row, "Program Name"),
                    "program_description": getattr(row, "Program description"),
                    "offers_price": getattr(
                        row, "Total cost of the program (in dollars)"
                    ),
                    "program_url": getattr(row, "URL of Program"),
                    "provider_name": getattr(row, "Goodwill Member Name"),
                    "provider_url": getattr(row, "Organization URL"),
                    "provider_telephone": getattr(
                        row, "Contact phone number for program"
                    ),
                    "provider_address": provider_address,
                    "time_to_complete": time_to_complete,
                    "identifier_cip": getattr(row, "CIP Code"),
                    "identifier_program_id": getattr(row, "Program ID"),
                    "start_date": getattr(
                        row, "Start date(s)"
                    ),  # I think this is a list?
                    "end_date": getattr(row, "End Date(s)"),  # I think this is a list?
                    "occupational_credential_awarded": getattr(
                        row,
                        "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?",
                    ),
                    "educational_program_mode": getattr(row, "Format"),
                    "maximum_enrollment": getattr(row, "Maximum Enrollment"),
                    "time_of_day": getattr(row, "Timing"),
                }

                prereq_blob = self._make_prereq_blob(row)
                if prereq_blob:
                    input_kwargs["program_prerequisites"] = prereq_blob

                try:
                    pathways_json_ld = educational_occupational_programs_converter(
                        **input_kwargs
                    )
                except ValueError as err:
                    logger.error(
                        f"Could not transform data for Google Sheet Row: {gs_row_identifier}. See below error."
                    )
                    logger.error(err)
                    continue

            yield [gs_row_identifier, getattr(row, "Timestamp"), pathways_json_ld]

    def pathways_transform(self):
        """This function consolidates the programs data into a "list of lists"
        (i.e., the same data type the Google API resource returns)."""
        list_of_lists = []

        for list_with_pathways_program in self._convert_to_pathways_json():
            list_of_lists.append(list_with_pathways_program)

        headers = ["id", "updated_at", "pathways_program"]
        dataframe_obj = pd.DataFrame(list_of_lists, columns=headers)

        return dataframe_obj
