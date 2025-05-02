from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

""" SQL database """
engines = {
    "endftables": create_engine("sqlite:////Users/okumuras/Documents/nucleardata/EXFOR/endftables.sqlite"),
}
Base = declarative_base()
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engines["endftables"]))
session = Session()
connection = engines["endftables"].connect()

# conn = sqlite3.connect('sqlite:////Users/okumuras/Documents/nucleardata/EXFOR/endftables.sqlite')


# MT_PATH_JSON = "/Users/okumuras/Dropbox/Development/exforparser/src/exforparser/tabulated/mt.json"
LIB_PATH = "/Users/okumuras/Documents/nucleardata/libraries.all/"


LIB_LIST = [
    "tendl.2023",
    # "tendl.2021",
    "endfb8.0",
    "eaf.2010", # European Activation File
    "jeff3.3",
    "jendl5.0",
    # "jendl4.0",
    "iaea.2022",
    # "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
    "ibandl",
]
