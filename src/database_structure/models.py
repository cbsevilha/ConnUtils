import os
from sqlalchemy import MetaData
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql

from dotenv import load_dotenv

load_dotenv(override=True)  # take environment variables from .env.

convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata_obj = MetaData(
    naming_convention=convention, schema=os.getenv("LOCAL_DB_SCHEMA")
)
Base = declarative_base(metadata=metadata_obj)


class RawDataFile(Base):
    """
    Table of Mixpanel Distinct Ids.

    """

    __tablename__ = "raw_data_files"
    __table_args__ = (UniqueConstraint("raw_file_hash"),)
    id = Column(
        postgresql.BIGINT,
        primary_key=True,
        autoincrement=True,
        nullable=False
    )
    raw_file_name = Column(
        postgresql.VARCHAR(512),
        nullable=False,
        index=True
    )
    raw_file_hash = Column(
        postgresql.VARCHAR(512),
        nullable=False,
        index=True
    )
    created_on = Column(
        postgresql.TIMESTAMP(timezone=True),
        server_default=func.now()
    )
    updated_on = Column(
        postgresql.TIMESTAMP(timezone=True),
        server_default=func.now(),
        server_onupdate=func.now(),
    )

