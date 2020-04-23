from datetime import datetime
from uuid import uuid4

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy import Column
from sqlalchemy.types import TIMESTAMP, String

Base = declarative_base()


class PathwaysProgram(Base):
    __tablename__ = "pathways_program"
    id = Column(String, primary_key=True)
    pathways_program = Column(JSONB)
    updated_at = Column(TIMESTAMP)

    def __init__(self, updated_at, pathways_program, id=None):
        self.updated_at = updated_at
        self.pathways_program = pathways_program
        self.id = id or str(uuid4())
