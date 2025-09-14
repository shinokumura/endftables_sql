import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker


BASE_DIR = Path(__file__).resolve().parent

ENV = "dev"  # or "INT" or "PROD"


if ENV == "dev":
    DATA_DIR = "/Users/okumuras/Documents/nucleardata/EXFOR/"

elif ENV == "int":
    DATA_DIR = "/srv/data/dataexplorer_v2/"

elif ENV == "prod":
    DATA_DIR = "/nds/data/dataexplorer_v2/"


ENDFTAB_DB = os.path.join(DATA_DIR, "endftables_.sqlite")

""" SQL database """
engines = {
    # "exfor": create_engine("sqlite:///" + EXFOR_DB),
    "endftables": create_engine("sqlite:///" + ENDFTAB_DB),
}

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

# obs_types = ["xs", "residual", "angle"]