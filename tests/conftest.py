import json
from time import sleep

import docker
import pytest
from sqlalchemy import MetaData, Table, Column, create_engine, insert, delete
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.engine import ResultProxy
from sqlalchemy.types import TIMESTAMP, String
from sqlalchemy.orm import sessionmaker

from config.config_test import (
    CONTAINER_NAME,
    POSTGRES_DATABASE,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SQLALCHEMY_DATABASE_URI,
)
from etl.pathways_opt_out import OptOut
from etl.transformers.dataframe_transformer import DataframeTransformer
from etl.utils.logger import logger


ENGINE = create_engine(SQLALCHEMY_DATABASE_URI)
# Create a MetaData object, which stores the components of the database being described or reflected
METADATA_OBJECT = MetaData(bind=ENGINE)


class PostgreSQLContainer:
    """A PostgreSQL Container Object.

    This class provides a mechanism for managing PostgreSQL Docker
    containers so that a database can be injected into tests.

    Class Attributes:
        container (object): The Docker container object.
        docker_client (object): Docker client.
        db_environment (list): Database environment configuration variables.
        db_ports (dict): Dictionary of database port mappings.
    """

    def __init__(self):
        self.container = None
        self.docker_client = docker.from_env()
        self.db_environment = [
            "POSTGRES_USER={}".format(POSTGRES_USER),
            "POSTGRES_PASSWORD={}".format(POSTGRES_PASSWORD),
            "POSTGRES_DB={}".format(POSTGRES_DATABASE),
        ]
        self.db_ports = {"5432/tcp": POSTGRES_PORT}

    def get_postgresql_image(self):
        """Output the PostgreSQL image from the configuration.

        Returns:
            str: The PostgreSQL image name and version tag.
        """
        return "{}:{}".format("postgres", "12")

    def start_container(self):
        """Start PostgreSQL Container."""
        if self.get_db_if_running():
            return

        try:
            self.docker_client.images.pull(self.get_postgresql_image())
        except Exception:
            logger.error("Failed to pull postgres image")
            raise RuntimeError

        self.container = self.docker_client.containers.run(
            self.get_postgresql_image(),
            detach=True,
            auto_remove=True,
            name=CONTAINER_NAME,
            environment=self.db_environment,
            ports=self.db_ports,
        )
        logger.info("PostgreSQL container running!")

        create_pathways_program_table()

    def stop_if_running(self):
        try:
            running = self.docker_client.containers.get(CONTAINER_NAME)
            logger.info(f"Killing running container '{CONTAINER_NAME}'")
            running.stop()
        except Exception as e:
            if "404 Client Error: Not Found" in str(e):
                return
            raise e

    def get_db_if_running(self):
        """Return the container or None."""
        try:
            return self.docker_client.containers.get(CONTAINER_NAME)
        except Exception as e:
            if "404 Client Error: Not Found" in str(e):
                return


def create_pathways_program_table():
    """Create a PathwaysProgram table in the database.

    This function instantiates a SQLAlchemy Table object and associates it with
    the METADATA_OBJECT. Then, the function waits for Docker to standup the database
    and calls `create_all`, which calls "CREATE TABLE" for all associated tables.
    https://docs.sqlalchemy.org/en/13/core/tutorial.html#define-and-create-tables
    """
    table_created = False
    retries = 0
    max_retries = 3

    programs_table = Table(
        "pathways_program",
        METADATA_OBJECT,
        Column("id", String, primary_key=True),
        Column("pathways_program", JSONB, nullable=False),
        Column("updated_at", TIMESTAMP, nullable=False),
    )

    while retries < max_retries and table_created is False:
        logger.info(
            "Attempting to apply migrations ({} of {})...".format(
                retries + 1, max_retries
            )
        )
        try:
            METADATA_OBJECT.create_all(ENGINE)
            table_created = True
        except Exception:
            retries += 1
            sleep(2)


@pytest.fixture(scope="session", autouse=True)
def pathways_program_table():
    return Table("pathways_program", METADATA_OBJECT, autoload=True)


@pytest.fixture(scope="session", autouse=True)
def database():
    postgres = PostgreSQLContainer()

    yield postgres.start_container()

    postgres.stop_if_running()


@pytest.fixture(autouse=True)
def database_session():
    Session = sessionmaker()
    Session.configure(bind=ENGINE)
    session = Session()

    yield session

    pathways_program_table = Table("pathways_program", METADATA_OBJECT, autoload=True)
    delete_stmt = delete(pathways_program_table)
    session.execute(delete_stmt)
    session.commit()
    session.close()


@pytest.fixture
def google_sheet_data():
    return [
        [
            "Timestamp",
            "Your email address",
            "Goodwill Member Name",
            "Organization URL",
            "Organization Address",
            "Program Name",
            "Program description",
            "Program ID",
            "Program Status",
            "Program Category",
            "Population(s) Targeted",
            "Goal/Outcome",
            "Time Investment",
            "Should this program be available in Google Pathways?",
            "URL of Program",
            "Program Address (if different from organization address)",
            "Contact phone number for program",
            "CIP Code",
            "Application Deadline",
            "Total cost of the program (in dollars)",
            "Duration / Time to complete",
            "Total Units",
            "Unit Cost (not required if total cost is given)",
            "Format",
            "Timing",
            "Start date(s)",
            "End Date(s)",
            "Credential level earned",
            "Accreditation body name",
            "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?",
            "What occupations/jobs does the training prepare you for?",
            "Apprenticeship or Paid Training Available",
            "If yes, average hourly wage paid to student",
            "Incentives",
            "Average ANNUAL salary post-graduation",
            "Average HOURLY wage post-graduation",
            "Eligible groups",
            "Maximum yearly household income to be eligible",
            "HS diploma required?",
            "Other prerequisites",
            "Anything else to add about the program?",
            "Maximum Enrollment",
            "Row Identifier (DO NOT EDIT)",
        ],
        [
            "03/18/2020 07:25:37",
            "john@goodwill.test",
            "Goodwill of Springfield",
            "http://www.goodwill.test/",
            "1 Grickle Grass Lane Springfield, MA 88883",
            "Youth Employment",
            "Work experience program for youth between the ages of 16-24",
            "5688",
            "Open",
            "Job Skills Training",
            "Youth",
            "Employment, Career Advancement, Certificate/Credential/Degree",
            "160 hours",
            "Yes",
            "http://www.goodwill.test/programs-and-services/one-stop-adult-employment/",
            "",
            "941-999-9999",
            "49.0444",
            "01/15/2021",
            "0",
            "",
            "",
            "",
            "In person",
            "Evenings, Weekends, Full-time",
            "07/15/2021",
            "12/15/2021",
            "",
            "",
            "",
            "N/A",
            "Yes",
            "11",
            "",
            "",
            "",
            "Youth",
            "",
            "No",
            "",
            "",
            "",
            "3f109a01-87c6-4899-bfd0-63db86acae11",
        ],
    ]


@pytest.fixture
def transformer(google_sheet_data):
    transformer = DataframeTransformer(sheet=google_sheet_data, engine=ENGINE)

    return transformer


@pytest.fixture
def opt_out(pathways_program_table, google_sheet_data):
    opt_out = OptOut(
        google_sheet_as_list=google_sheet_data,
        programs_table=pathways_program_table,
        engine=ENGINE,
    )

    return opt_out


@pytest.fixture
def pathways_programs(pathways_program_table, database_session):
    """A fixture that adds two programs to the database transaction (i.e.,
    SQLAlchemy session)."""
    json_ld_one = {
        "@context": "http://schema.org/",
        "@type": "EducationalOccupationalProgram",
        "name": "Certified Nursing Assistant Program",
        "description": "This course helps participants cultivate the attitudes, skills and behaviors of a competent caregiver",
    }

    program_data_one = {
        "id": "5f109a01-87c6",
        "updated_at": "2020-03-23 15:10:50",
        "pathways_program": json.dumps(json_ld_one),
    }

    json_ld_two = {
        "@context": "http://schema.org/",
        "@type": "WorkBasedProgram",
        "name": "Customer Service and Sales Training",
        "description": "Provides training in Customer Service and Sales",
    }

    program_data_two = {
        "id": "663dfe-4aca",
        "updated_at": "2020-02-01 1:31:10",
        "pathways_program": json.dumps(json_ld_two),
    }

    # Create a SQLAlchemy Insert object
    insert_obj = insert(pathways_program_table)
    insert_obj_with_values = insert_obj.values([program_data_one, program_data_two])
    database_session.execute(insert_obj_with_values)
    database_session.commit()


@pytest.fixture
def connection_mock():
    class ConnectionMock:
        def __enter__(self):
            return self

        def __exit__(self, type, value, tb):
            pass

        def execute(self, query):
            return ResultProxy

    return ConnectionMock()
