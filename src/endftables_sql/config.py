import sqlalchemy as db
from sqlalchemy.orm import scoped_session, sessionmaker

""" SQL database """

engine = db.create_engine(
    # "sqlite:////Users/okumuras/Desktop/endftables.sqlite"
    "sqlite:////Users/okumuras/Documents/nucleardata/EXFOR/endftables.sqlite"
)  # , echo=True)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
session = Session()

# session_lib = sessionmaker(autocommit=False, autoflush=True, bind=engine)

MT_PATH_JSON = "/Users/okumuras/Dropbox/Development/exforparser/src/exforparser/tabulated/mt.json"
LIB_PATH = "/Users/okumuras/Documents/nucleardata/EXFOR/libraries.all/"


LIB_LIST = [
    "tendl.2021",
    "endfb8.0",
    "eaf.2010", # European Activation File
    "jeff3.3",
    "jendl5.0",
    "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
]
