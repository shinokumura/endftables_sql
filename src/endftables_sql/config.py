from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

""" SQL database """
engines = {
    "endftables": create_engine("sqlite:////Users/okumuras/Documents/nucleardata/EXFOR/endftables.sqlite"),
}
Base = declarative_base()
session_lib = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engines["endftables"]))

BATCH_SIZE = 1000
# MT_PATH_JSON = "/Users/okumuras/Dropbox/Development/exforparser/src/exforparser/tabulated/mt.json"
LIB_PATH = "/Users/okumuras/Documents/nucleardata/libraries.all/"
FPY_LIB_PATH = "/Users/okumuras/Dropbox/Development/tabfylibs/FY"

LIB_LIST = [
    "tendl.2023",
    # "tendl.2024",
    # "tendl.2021",
    # "endfb8.0",
    "endfb8.1",
    "eaf.2010", # European Activation File
    # "jeff3.3",
    "jeff4.0",
    "jendl5.0",
    # "jendl4.0",
    "iaea.2022",
    # "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
    "ibandl",
]
