import os
from database_structure.models import Base
from sqlalchemy import schema

from helpers.database import DBConnectorFactory

from dotenv import load_dotenv

load_dotenv(override=True)  # take environment variables from .env.


db_factory = DBConnectorFactory()
# TODO: CHANGE TO ANALYTICS
db = db_factory.get_db_connector(db_instance_name="local_db", verbose=True)


# CREATE THE TABLES
with db.engine.begin() as connection:
    connection.execute(schema.CreateSchema(os.getenv("LOCAL_DB_SCHEMA")))
    Base.metadata.create_all(bind=connection)
