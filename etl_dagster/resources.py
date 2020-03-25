# Resource – typically, an external connection, e.g., to a database
# Available via context variable
# BUT: you have to tell the solid which resource you require

from google.oauth2.service_account import Credentials
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from dagster import (
    resource,
    Field,
    Shape,
    StringSource,
    Array,
    List,
    Dict,
    String
)

@resource(
    {
        'google_account_info': Field(
            Shape({
                "type": Field(StringSource),
                "project_id": Field(StringSource),
                "private_key_id": Field(StringSource),
                "private_key": Field(StringSource),
                "client_email": Field(StringSource),
                "client_id": Field(StringSource),
                "auth_uri": Field(StringSource),
                "token_uri": Field(StringSource),
                "auth_provider_x509_cert_url": Field(StringSource),
                "client_x509_cert_url": Field(StringSource)
            })
        )
    }
)
def googleapi_resource(init_context):
    """
    This function builds a googleapiclient Resource, i.e.,
    an object that can interact with an api, such as Google sheets ("sheets").

    https://github.com/googleapis/google-api-python-client/blob/5c11b0a1b2658b26fe41b13ebd2e9e7b53c1ab01/googleapiclient/discovery.py#L170
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/documents.readonly",
    ]
    # Get google_account_info from environment config which is defined in our resource decorator above
    google_account_info = init_context.resource_config['google_account_info']
    credentials = Credentials.from_service_account_info(google_account_info, scopes=scopes)

    return discovery.build(
        'sheets',
        'v4',
        credentials=credentials,
        cache_discovery=False,
    )


@resource(
    {
        'sheets': Field(
            Array(

                {
                    "id": Field(String),
                    "value": Field(String)
                }

            )
        )
    }
)
def spreadsheet_resource(init_context):
    spreadsheet_info = init_context.resource_config['sheets']

    return spreadsheet_info