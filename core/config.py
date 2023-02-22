from environs import Env

env = Env()
env.read_env(".env")

# Application
APP_NAME = env("APP_NAME")
APP_VERSION = env("APP_VERSION")

# Database
SQLALCHEMY_DATABASE_URI = "postgresql+asyncpg://{}:{}@{}:{}/{}".format(
    "user",  # TODO add db username
    "pass",  # TODO add db password
    "0.0.0.0",  # TODO add db host
    "5432",  # TODO add db port
    "",  # TODO add db name
)
