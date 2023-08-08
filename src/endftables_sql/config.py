import sqlalchemy as db
from sqlalchemy.orm import scoped_session, sessionmaker

""" SQL database """

engine = db.create_engine(
    "sqlite:////Users/sin/Desktop/endftables-ongoing.sqlite"
)  # , echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


MT_PATH_JSON = "/Users/sin/Dropbox/Development/exforparser/tabulated/mt.json"
# LIB_PATH = "/Users/okumuras/Documents/nucleardata/libraries.plot/"
LIB_PATH = "/Users/sin/Documents/nucleardata/libraries.plot/"

LIB_LIST = [
    "tendl.2021",
    "endfb8.0",
    "jeff3.3",
    "jendl5.0",
    "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
]
