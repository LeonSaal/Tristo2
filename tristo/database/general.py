from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ..paths import HOME
from .tables import Base


class OpenDB:
    def __init__(self) -> None:
        self.name = f"sqlite:///{HOME.as_posix()}/db.db"
        self.engine = create_engine(self.name)
        Base.metadata.create_all(self.engine)

    def session(self) -> Session:
        return Session(self.engine)
