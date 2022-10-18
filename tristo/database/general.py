from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..paths import HOME
from .tables import Base


class OpenDB:
    def __init__(self) -> None:
        self.engine = create_engine(f"sqlite:///{HOME}/db.db")
        Base.metadata.create_all(self.engine)

    def session(self):
        return Session(self.engine)
