import pytest
from sqlalchemy import create_engine

from config.config import SQLALCHEMY_DATABASE_URI
from etl.transformers.transformer import Transformer

@pytest.fixture
def google_sheet_data():
    return [
        [
            'Timestamp', 
            'Your email address',
            'Goodwill Member ID',
            'Organization Name',
            'Organization URL',
            'Organization Address',
            'Program Name',
            'Program description',
            'Program ID',
            'Program Status',
            'Program Category',
            'Population(s) Targeted',
            'Goal/Outcome',
            'Time Investment',
            'Should this program be available in Google Pathways?',
            'URL of Program',
            'Program Address (if different from organization address)',
            'Contact phone number for program',
            'CIP Code',
            'Application Deadline',
            'Total cost of the program (in dollars)',
            'Duration / Time to complete',
            'Total Units',
            'Unit Cost (not required if total cost is given)',
            'Format',
            'Timing',
            'Start date(s)',
            'End Date(s)',
            'Credential level earned',
            'Accreditation body name',
            'What certification (exam), license, or certificate (if any) does this program prepare you for or give you?',
            'What occupations/jobs does the training prepare you for?',
            'Apprenticeship or Paid Training Available',
            'If yes, average hourly wage paid to student',
            'Incentives',
            'Average ANNUAL salary post-graduation',
            'Average HOURLY wage post-graduation',
            'Eligible groups',
            'Maximum yearly household income to be eligible',
            'HS diploma required?',
            'Other prerequisites',
            'Anything else to add about the program?',
            'Maximum Enrollment',
            'Row Identifier (DO NOT EDIT)'
        ],
        [
            '03/18/2020 07:25:37',
            'john@goodwill.test',
            'Springfield, MA',
            'Goodwill of Springfield',
            'http://www.goodwill.test/',
            '1 Grickle Grass Lane Springfield, MA 88883',
            'Youth Employment',
            'Work experience program for youth between the ages of 16-24',
            '5688',
            'Discontinued',
            '',
            'Youth',
            '',
            '',
            'Yes',
            'http://www.goodwill.test/programs-and-services/one-stop-adult-employment/',
            '',
            '',
            '',
            '01/15/2021',
            '0',
            '',
            '',
            '',
            'In person',
            'Evenings, Weekends, Full-time',
            '07/15/2021',
            '12/15/2021',
            '',
            '',
            '',
            'N/A',
            'Yes',
            '11',
            '',
            '',
            '',
            'Youth',
            '',
            'No',
            '',
            '',
            '',
            '3f109a01-87c6-4899-bfd0-63db86acae11'
        ],
    ]


@pytest.fixture
def transformer(google_sheet_data):
    engine = create_engine(SQLALCHEMY_DATABASE_URI)
    transformer = Transformer(sheet=google_sheet_data, engine=engine, spreadsheet_id='12345678')

    return transformer
