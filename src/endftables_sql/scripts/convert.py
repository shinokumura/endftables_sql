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


FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


from endftables_sql.config import engines, session_lib, LIB_LIST, LIB_PATH, FPY_LIB_PATH
from sqlalchemy import update, insert, Table, MetaData, select, and_, or_


from endftables_sql.submodules.utilities.elem import ztoelem
from endftables_sql.submodules.utilities.reaction import mt_to_process
metadata = MetaData()

def check(connection, endf_reactions, p, nuclide, lib, type, en_inc, mt, residual):

    stmt = select(endf_reactions.c.points).where(
        and_(
        endf_reactions.c.projectile == p,
        endf_reactions.c.target == nuclide,
        endf_reactions.c.evaluation == lib,
        endf_reactions.c.type == type,
        endf_reactions.c.en_inc == en_inc,
        endf_reactions.c.mt == mt,
        endf_reactions.c.residual == residual,
    ))

    result = connection.execute(stmt).fetchall()

    if len(result) == 0:
        return -1
    else:
        return len(result)




def insert_index(connection, endf_reactions, projectile, nuclide, lib, type, en_inc, mt, residual):
    
    react = {
        "evaluation": lib,
        "type": type,
        "target": nuclide,
        "projectile": projectile,
        "en_inc":  en_inc,
        "process": mt_to_process(projectile, type, mt),
        "residual":  residual,
        "mf": 3 if type == "xs" else 8 if type == "fy" else 4 if type == "angle" else 10 if type == "residual" else None,
        "mt":  str(int(mt)) if mt else None
    }
    stmt = insert(endf_reactions).values(react).returning(endf_reactions.c.reaction_id)
    # result = connection.execute(stmt)
    # # result.close()
    # # connection.commit()
    # reaction_id = result.scalar_one()
    # print(reaction_id)
    # connection.commit()
    # connection.close()
    return connection.execute(stmt).scalar_one()





def process_all(type, projectile, nfl):
    if type == "fy":
        nuclides = [
            d
            for d in os.listdir(os.path.join(FPY_LIB_PATH, projectile))
            if os.path.isdir(os.path.join(FPY_LIB_PATH, projectile, d))
        ]
        types = ["fy"]

    else:
        nuclides = [
            d
            for d in os.listdir(os.path.join(LIB_PATH, projectile))
            if re.match(f"[{nfl}]", d[0]) and os.path.isdir(os.path.join(LIB_PATH, projectile, d)) 
        ]
        types = ["xs", "residual", "angle"]

    nuclides = sorted(nuclides)

    # with engines["endftables"].begin() as connection: 
    #     endf_reactions = Table("endf_reactions", metadata, autoload_with=connection.engine)


    for nuclide in nuclides:
        connection = engines["endftables"].connect()
        trans = connection.begin()
        try:
            endf_reactions = Table("endf_reactions", metadata, autoload_with=connection)

            for lib in LIB_LIST:
                for type in types:
                    files = []

                    if type == "fy":
                        table_path = os.path.join(
                            FPY_LIB_PATH, projectile, nuclide, lib, "tables", "FY"
                        )

                    else:
                        table_path = os.path.join(LIB_PATH, projectile, nuclide, lib, "tables", type)

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
                        continue

                    if not files:
                        print(f"no files in {table_path}")
                        continue

                    print("Processing: ", projectile, nuclide, lib, type)
                    for file in files:
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

                        if type == "xs":
                            if name[2].endswith(('m','g','n','l')):
                                # In case if the file is for the production of metastable or ground explicitly given 
                                # (They are included as residual production cross sections)
                                continue

                            mt = re.sub(r"\D", "", name[2])
                            residual = None


                        elif type == "residual":
                            if len(name[2]) >= 8:
                                elem = ztoelem(int(name[2][2:5]))
                                mass = str(int(name[2][5:8])).zfill(3)
                                residual = elem + mass
                            if len(name[2]) == 9:
                                iso = str(name[2][-1])
                                residual = elem + mass.zfill(3) + iso

                            mt = None


                        elif type == "fy":
                            mt = re.sub(r"\D", "", name[2])
                            residual = None
                            en1 = re.sub(r'\D', '', name[3]).replace('E','')
                            en2 = re.sub(r'\D', '', name[4])
                            if en1 == "2.5E":
                                en_inc = 2.5E-8
                            else:
                                en_inc = float( f"{en1}.{en2}" )


                        elif type == "angle":
                            ## angle:    ['n', 'Ac222', 'MT056', 'Eang200', '000', 'tendl', '2021', 'txt']
                            ##           ['n', 'Ac222m', 'MT002', 'Eang1', '0E', '11', 'tendl', '2023', 'txt']
                            ##           ['n', 'Ac222m', 'MT002', 'Eang1', '0E', '04', 'tendl', '2023', 'txt']
                            mt = re.sub(r"\D", "", name[2])
                            residual = None
                            if len(name) == 8:
                                en_inc = float ( f"{name[3].replace('Eang','')}.{name[4]}" )
                            elif len(name) == 9:
                                en_inc = float ( f"{name[3].replace('Eang','')}.{name[4]}-{name[5]}" )

                        try:
                            count = check(connection, endf_reactions, projectile, nuclide, lib, type, en_inc, mt, residual)
                            if count == -1:
                                # print("Processing: ", projectile, nuclide, lib, type, en_inc, mt, residual )
                                read_libs(connection, endf_reactions, projectile, nuclide, lib, type, en_inc, mt, residual, file)

                            if count > 0:
                                print(projectile, nuclide, lib, type, en_inc, mt, residual, "exist")
                                continue

                        except KeyboardInterrupt:
                            print("CTR + C")
                            trans.rollback()
                            connection.close()
                            raise
                    
                        except Exception:
                            logging.error(f"ERROR: at file: {file}", exc_info=True)
                            trans.rollback() 
                            trans = connection.begin()

            trans.commit()

        except Exception:
            trans.rollback()
            logging.error(f"Fatal error at nuclide: {nuclide}", exc_info=True)

        finally:
            connection.close()



def read_libs(connection, endf_reactions, projectile, nuclide, lib, type, en_inc, mt, residual, file):
    lib_df = pd.DataFrame()

    # endf_reactions = Table("endf_reactions", metadata, autoload_with=connection.engine)
    reaction_id = insert_index(connection, endf_reactions, projectile, nuclide, lib, type, en_inc, mt, residual)
    
    if type == "xs":
        lib_df = create_libdf(file, reaction_id)
        lib_df.to_sql(
            "endf_xs_data",
            connection,
            index=False,
            if_exists="append",
            method='multi' 
        )

    elif type == "residual" and projectile == "n":
        lib_df = create_libdf(file, reaction_id)
        lib_df.to_sql(
            "endf_n_residual_data",
            connection,
            index=False,
            if_exists="append",
            method='multi' 
        )


    elif type == "residual" and projectile != "n":
        lib_df = create_libdf(file, reaction_id)
        lib_df.to_sql(
            "endf_residual_data",
            connection,
            index=False,
            if_exists="append",
            method='multi' 
        )

    elif type == "fy":
        lib_df, en_inc = create_libdf_fy(file, reaction_id)
        lib_df.to_sql(
            "endf_fy_data",
            connection,
            index=False,
            if_exists="append",
            method='multi' 
        )

    elif type == "angle":
        lib_df, en_inc = create_libdf_angle(file, reaction_id)
        lib_df.to_sql(
            "endf_angle_data",
            connection,
            index=False,
            if_exists="append",
            method='multi' 
        )

    else:
        pass

    # print(reaction_id)
    # print(lib_df)

    stmt = (
        update(endf_reactions).where(endf_reactions.c.reaction_id == reaction_id).values(points=len(lib_df.index))
    )
    connection.execute(stmt)
    # connection.commit()
    # connection.close()

    # session_lib.query(Endf_Reactions).filter(Endf_Reactions.reaction_id == reaction_id).update({"points" : len(lib_df.index)})
    # session_lib.commit()
    # session_lib.close()

    return 




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
        en_inc = float(f.readlines()[18].split(":")[1].strip())/1E+6

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

    lib_df = lib_df.reset_index()
    lib_df = lib_df.drop("index", axis=1)

    return lib_df, en_inc





def show_reaction():
    with engines["endftables"].connect() as connection:
    # connection = engine.connect()
        data = session_lib.query(Endf_Reactions)

        df = pd.read_sql(
            sql=data.statement,
            con=connection,
        )


def show_data():
    with engines["endftables"].connect() as connection:
    # connection = engine.connect()
        data = session_lib.query(Endf_XS_Data)

        df = pd.read_sql(
            sql=data.statement,
            con=connection,
        )

    return df


if __name__ == "__main__":
    read_libs()
    # try:
    #     read_libs()
    # except:
    #     pass
