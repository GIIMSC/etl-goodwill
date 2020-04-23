from time import sleep

import docker
import pytest
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.engine import ResultProxy
from sqlalchemy.types import TIMESTAMP, String

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


class PostgreSQLContainer:
    """A PostgreSQL Container Object.

    This class provides a mechanism for managing PostgreSQL Docker
    containers so that a database can be injected into tests. Class
    Attributes:     config (object): A Configuration Factory object.
    container (object): The Docker container object.     docker_client
    (object): Docker client.     db_environment (list): Database
    environment configuration variables.     db_ports (dict): Dictionary
    of database port mappings.
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
    """Apply Database Migrations Applies the database migrations to the test
    database container.

    Args:
        config (object): Configuration object to pull the needed components from.
    """
    table_created = False
    retries = 0
    max_retries = 3
    metadata = MetaData()

    programs_table = Table(
        "pathways_program",
        metadata,
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
            metadata.create_all(ENGINE)
            table_created = True
        except Exception:
            retries += 1
            sleep(2)


@pytest.fixture(scope="session", autouse=True)
def database():
    postgres = PostgreSQLContainer()
    yield postgres.start_container()
    postgres.stop_if_running()


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
def opt_out(google_sheet_data):
    metadata = MetaData(bind=ENGINE)
    programs_table = Table("pathways_program", metadata)
    opt_out = OptOut(
        google_sheet_as_list=google_sheet_data,
        programs_table=programs_table,
        engine=ENGINE,
    )

    return opt_out


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
