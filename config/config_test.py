POSTGRES_USER = "brighthive_admin"
POSTGRES_PASSWORD = "password"
POSTGRES_HOSTNAME = "localhost"
POSTGRES_PORT = "10031"
POSTGRES_DATABASE = "pathways"
SQLALCHEMY_DATABASE_URI = "postgresql://{}:{}@{}:{}/{}".format(
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOSTNAME,
    POSTGRES_PORT,
    POSTGRES_DATABASE,
)

CONTAINER_NAME = "test_postgres_service"
