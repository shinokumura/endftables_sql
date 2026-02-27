####################################################################
#
# This file is part of libraries-2023 dataexplorer, https://nds.iaea.org/dataexplorer/.
# Copyright (C) 2023 International Atomic Energy Agency (IAEA)
#
# Contact:    nds.contact-point@iaea.org
#
####################################################################

import pandas as pd
import os
import re
import logging

from endftables_sql.config import engines, session_lib, LIB_LIST, LIB_PATH, FPY_LIB_PATH
from endftables_sql.scripts.models_core import endf_reactions
from sqlalchemy import update, insert, Table, MetaData, select, and_, or_, text, event
from sqlalchemy.exc import IntegrityError

from endftables_sql.submodules.utilities.elem import ztoelem
from endftables_sql.submodules.utilities.reaction import mt_to_process

metadata = MetaData()


@event.listens_for(engines["endftables"], "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

def check(connection, endf_reactions, p, nuclide, lib, obs_type, en_inc, mt, residual):

    stmt = select(endf_reactions.c.points).where(
        and_(
        endf_reactions.c.projectile == p,
        endf_reactions.c.target == nuclide,
        endf_reactions.c.evaluation == lib,
        endf_reactions.c.obs_type == obs_type,
        endf_reactions.c.en_inc == en_inc,
        endf_reactions.c.mt == mt,
        endf_reactions.c.residual == residual,
    ))

    result = connection.execute(stmt).fetchall()

    if len(result) == 0:
        return -1
    else:
        return len(result)



logger = logging.getLogger(__name__)
def setup_logger(projectile, suffix=""):
    log_filename = f"process_{projectile}{suffix}.log"
    root_logger = logging.getLogger()
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8", delay=False)
    file_handler.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.WARNING) 

    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)



def insert_index(connection, endf_reactions, projectile, nuclide, lib, obs_type, en_inc, mt, residual):
    v_en_inc = en_inc if en_inc is not None else 0.0
    v_residual = residual if residual else ""
    v_mt = int(mt) if mt is not None else 0

    react = {
        "evaluation": lib,
        "obs_type": obs_type,
        "target": nuclide,
        "projectile": projectile,
        "en_inc": v_en_inc,
        "process": mt_to_process(projectile, obs_type, mt),
        "residual": v_residual,
        "mf": 3 if obs_type == "xs" else 8 if obs_type == "fy" else 4 if obs_type == "angle" else 10 if obs_type == "residual" else None,
        "mt": v_mt
    }

    result = connection.execute(insert(endf_reactions).values(react))
    return result.lastrowid
    # stmt = insert(endf_reactions).values(react).returning(endf_reactions.c.reaction_id)
    # return connection.execute(stmt).scalar_one()


def glob_nuclides_from_liball(run_type, projectile, nfl):
    if run_type == "fy":
        nuclides = [
            d
            for d in os.listdir(os.path.join(FPY_LIB_PATH, projectile))
            if os.path.isdir(os.path.join(FPY_LIB_PATH, projectile, d))
        ]
        obs_types = ["fy"]

    else:
        nuclides = [
            d
            for d in os.listdir(os.path.join(LIB_PATH, projectile))
            if re.match(f"[{nfl}]", d[0]) and os.path.isdir(os.path.join(LIB_PATH, projectile, d)) 
        ]
        obs_types = ["xs", "residual", "angle"]

    # nuclides = sorted(nuclides)
    return obs_types, sorted(nuclides)


def glob_files_from_liball(projectile, nuclide, lib, obs_type):
    files = []

    if obs_type == "fy":
        table_path = os.path.join(
            FPY_LIB_PATH, projectile, nuclide, lib, "tables", "FY"
        )

    else:
        table_path = os.path.join(LIB_PATH, projectile, nuclide, lib, "tables", obs_type)

    if os.path.exists(table_path):
        files = [
            os.path.join(table_path, f)
            for f in os.listdir(table_path)
            if os.path.isfile(os.path.join(table_path, f))
            and not any(
                w in f
                for w in (
                    "sacs",
                    "G1102",
                    ".DS_Store",
                    "MF",
                    ".list",
                    "YA",
                )
            )
        ]
    else:
        return None

    if not files:
        print(f"no files in {table_path}")
        return None

    # print("Processing: ", projectile, nuclide, lib, obs_type)

    return files


def extract_info_from_fn(file, obs_type):
    
    mt = None
    residual = None
    iso = None
    en_inc = None

    name = re.split("-|\.", os.path.basename(file))
    ## xs:       ['n', 'Ac222m', 'MT018', 'tendl', '2021', 'txt']
    ##           ['n', 'Ac222m', 'MT078', 'tendl', '2021', 'txt']
    ##           ['n', 'Ac222m', 'MT062', 'tendl', '2021', 'txt'] 
    ## residual: ['n', 'Ac222m', 'rp087204n', 'tendl', '2021', 'txt']
    ## angle:    ['n', 'Ac222', 'MT056', 'Eang200', '000', 'tendl', '2021', 'txt']

    if obs_type == "xs":
        if name[2].endswith(('m','g','n','l')):
            # In case if the file is for the production of metastable or ground explicitly given 
            # (They are included as residual production cross sections)
            return None, None, None

        mt = re.sub(r"\D", "", name[2])
        residual = None


    elif obs_type == "residual":
        if len(name[2]) >= 8:
            elem = ztoelem(int(name[2][2:5]))
            mass = str(int(name[2][5:8])).zfill(3)
            residual = elem + mass
        if len(name[2]) == 9:
            iso = str(name[2][-1])
            residual = elem + mass.zfill(3) + iso

        mt = None


    elif obs_type == "fy":
        mt = re.sub(r"\D", "", name[2])
        residual = None
        en1 = re.sub(r'\D', '', name[3]).replace('E','')
        en2 = re.sub(r'\D', '', name[4])
        if en1 == "2.5E":
            en_inc = 2.5E-8
        else:
            en_inc = float( f"{en1}.{en2}" )


    elif obs_type == "angle":
        ## angle:    ['n', 'Ac222', 'MT056', 'Eang200', '000', 'tendl', '2021', 'txt']
        ##           ['n', 'Ac222m', 'MT002', 'Eang1', '0E', '11', 'tendl', '2023', 'txt']
        ##           ['n', 'Ac222m', 'MT002', 'Eang1', '0E', '04', 'tendl', '2023', 'txt']
        mt = re.sub(r"\D", "", name[2])
        residual = None
        if len(name) == 8:
            en_inc = float ( f"{name[3].replace('Eang','')}.{name[4]}" )
        elif len(name) == 9:
            en_inc = float ( f"{name[3].replace('Eang','')}.{name[4]}-{name[5]}" )

    return mt, residual, en_inc




def process_all(run_type, projectile, nfl):
    suffix = f"_{nfl}" if projectile == "n" else ""
    setup_logger(projectile, suffix)
    
    obs_types, nuclides = glob_nuclides_from_liball(run_type, projectile, nfl)

    with open("false_complete") as pf:
        false_comp = {line.strip() for line in pf if line.strip()}

    with open("partial_sucess") as pf:
        partial_sucess = {line.strip() for line in pf if line.strip()}

    need_to_process = false_comp - partial_sucess
    print(need_to_process)

    for nuclide in nuclides:
        if nuclide not in need_to_process:
            # logger.info(f"Already processed: {nuclide}")
            # print(nuclide)
            continue
        processed_count = 0
        print(nuclide)
        try:
            with engines["endftables"].connect() as conn:
                with conn.begin(): 
                    conn.execute(text("BEGIN EXCLUSIVE TRANSACTION"))
                    for lib in LIB_LIST:
                        for obs_type in obs_types:
                            files = glob_files_from_liball(projectile, nuclide, lib, obs_type)
                            if not files:
                                continue

                            for file in files:
                                mt, residual, en_inc = extract_info_from_fn(file, obs_type)
                                
                                safe_mt = int(mt) if mt is not None else 0
                                safe_en_inc = float(en_inc) if en_inc is not None else 0.0
                                safe_residual = str(residual) if residual is not None else ""

                                rid = insert_index(
                                    conn, endf_reactions, projectile, nuclide, lib, 
                                    obs_type, safe_en_inc, safe_mt, safe_residual
                                )

                                if rid:
                                    read_libs(conn, rid, projectile, obs_type, file)
                                    processed_count += 1


            if processed_count >= 0:
                logger.info(f"Completed: {nuclide} ({processed_count} files)")

        except Exception as e:

            logger.error(f"Error in {nuclide}: {e}")
            continue




def process_one_file(projectile, nuclide, lib, obs_type, file):
    mt, residual, en_inc = extract_info_from_fn(file, obs_type)
    print("Processing:", projectile, nuclide, lib, obs_type)
    
    connection = None
    trans = None
    
    try:
        connection = engines["endftables"].connect()
        trans = connection.begin()
        # endf_reactions = Table("endf_reactions", metadata, autoload_with=connection)
        
        count = check(connection, endf_reactions, projectile, nuclide, lib, obs_type, en_inc, mt, residual)
        
        if count == -1:
            # print("Processing: ", projectile, nuclide, lib, obs_type, en_inc, mt, residual )
            read_libs(connection, endf_reactions, projectile, nuclide, lib, obs_type, en_inc, mt, residual, file)
            trans.commit()  
            
        elif count > 0:
            print(projectile, nuclide, lib, obs_type, en_inc, mt, residual, "exist")
            logging.info(f"file: {file} already exist", exc_info=True)
            trans.commit()  # to avoid too many connections, commit even if there is data
            
    except KeyboardInterrupt:
        print("CTR + C")
        if trans:
            trans.rollback()
        raise
        
    except Exception as e:
        logging.error(f"ERROR: at file: {file}", exc_info=True)
        if trans:
            trans.rollback()
        
    finally:
        # resource must be released
        if connection:
            connection.close()
    
    return None

def read_libs(connection, reaction_id, projectile, obs_type, file):
    """
    確定した reaction_id に対して、ファイルから読み込んだ数値データを紐付ける。
    """
    if obs_type == "xs":
        lib_df = create_libdf(file, reaction_id)
        lib_df.to_sql("endf_xs_data", connection, index=False, if_exists="append", method='multi', chunksize=500)

    elif obs_type == "residual":
        lib_df = create_libdf(file, reaction_id)
        table = "endf_n_residual_data" if projectile == "n" else "endf_residual_data"
        lib_df.to_sql(table, connection, index=False, if_exists="append", method='multi', chunksize=500)

    elif obs_type == "fy":
        lib_df, _ = create_libdf_fy(file, reaction_id)
        lib_df.to_sql("endf_fy_data", connection, index=False, if_exists="append", method='multi', chunksize=500)

    elif obs_type == "angle":
        lib_df, _ = create_libdf_angle(file, reaction_id)
        lib_df.to_sql("endf_angle_data", connection, index=False, if_exists="append", method='multi', chunksize=500)

    stmt = update(endf_reactions).where(endf_reactions.c.reaction_id == reaction_id).values(points=len(lib_df.index))
    connection.execute(stmt)



def create_libdf(libfile, reaction_id):
    lib_df = pd.DataFrame(columns=["en_inc", "data", "xslow", "xsupp",])
    try:
        lib_df = pd.read_table(
            libfile,
            sep="\s+",
            index_col=None,
            header=None,
            usecols=[0, 1, 2, 3],
            comment="#",
            names=[
                "en_inc",
                "data",
                "xslow",
                "xsupp",
            ],  # xslow/xsupp are only in tendl.2021, data in mb and en in MeV
        )
        lib_df["xslow"] *= 1e-3

        lib_df["xsupp"] *= 1e-3

    except:
        lib_df = pd.read_table(
            libfile,
            sep="\s+",
            index_col=None,
            header=None,
            usecols=[0, 1],
            comment="#",
            names=[
                "en_inc",
                "data",
            ]
        )

    ## Because Exfortables stores the data in mb
    lib_df["data"] *= 1e-3
    lib_df["reaction_id"] = reaction_id

    if lib_df["en_inc"].sum() == 0:
        lib_df.drop(lib_df[(lib_df["reaction_id"] == reaction_id)].index, inplace=True)

    lib_df = lib_df.reset_index()
    lib_df = lib_df.drop("index", axis=1)

    return lib_df





def create_libdf_fy(libfile, reaction_id):
    with open(libfile, "r") as f:
        en_inc = float(f.readlines()[16].split(":")[1].strip())/1E+6


    lib_df = pd.read_csv(
        libfile,
        sep="\s+",
        index_col=None,
        header=None,
        usecols=[0, 1, 2, 3, 4],
        comment="#",
        names=["charge", "mass", "isomeric", "data", "ddata"]
    )

    lib_df["en_inc"] = en_inc
    lib_df["reaction_id"] = reaction_id

    lib_df = lib_df.reset_index()
    lib_df = lib_df.drop("index", axis=1)

    pd.options.display.float_format = '{:12.5e}'.format

    return lib_df, en_inc



def create_libdf_angle(libfile, reaction_id):
    with open(libfile, "r") as f:
        en_inc = float(f.readlines()[18].split(":")[1].strip())

    lib_df = pd.read_csv(
        libfile,
        sep="\s+",
        index_col=None,
        header=None,
        usecols=[0, 1],
        comment="#",
        names=["angle", "data"],  # only in tendl.2021
    )

    lib_df["en_inc"] = en_inc
    lib_df["reaction_id"] = reaction_id

    ## Because Exfortables stores the data in mb/sr
    lib_df["data"] *= 1e-3

    lib_df = lib_df.reset_index()
    lib_df = lib_df.drop("index", axis=1)

    return lib_df, en_inc






if __name__ == "__main__":
    read_libs()
    # try:
    #     read_libs()
    # except:
    #     pass
