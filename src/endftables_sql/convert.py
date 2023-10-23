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
import json
import logging
import sys

if len(sys.argv) > 1:
    projectiles = list(sys.argv[1])


FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
logging.basicConfig(filename="processed.log", level=logging.DEBUG, filemode="w")

from .models import Endf_Reactions, Endf_XS_Data, Endf_Residual_Data, Endf_N_Residual_Data, Endf_FY_Data, Endf_ANGLE_Data
from .config import session, engine, MT_PATH_JSON, LIB_LIST, LIB_PATH
from .submodules.utilities.elem import ELEMS, ztoelem



def show_reaction():
    connection = engine.connect()
    data = session.query(Endf_Reactions)

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )
    # print(df)

    # for d in data:
    #     print(d.reaction_id, d.evaluation, d.target, d.projectile, d.process, d.mf, d.mt)


def show_data():
    connection = engine.connect()
    data = session.query(Endf_XS_Data)

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )
    # print(df)

    # for d in data:
    #     print( d.id, d.reaction_id, d.en_inc, d.data, d.ddata_1, d.ddata_2)


def check(p, nuclide, lib, type, mt, residual):
    reac = (
        session
        .query(Endf_Reactions)
        .filter(
            Endf_Reactions.projectile == p,
            Endf_Reactions.target == nuclide,
            Endf_Reactions.evaluation == lib,
            Endf_Reactions.type == type,
            Endf_Reactions.mt == mt,
            Endf_Reactions.residual == residual,
        )
        .all()
    )
    # print(len(reac))
    # print(reac)

    if len(reac) == 0:
        return len(reac)

    else:
        ## check the existence of data in the datatable

        for r in reac:
            # print(r.reaction_id, r.evaluation, r.target, r.projectile, r.process, r.residual, r.mt)

            if type == "fy":
                data = (
                    session
                    .query(Endf_FY_Data)
                    .filter(
                        Endf_FY_Data.reaction_id == r.reaction_id,
                    )
                    .all()
                )

            elif type == "xs":
                data = (
                    session
                    .query(Endf_XS_Data)
                    .filter(
                        Endf_XS_Data.reaction_id == r.reaction_id,
                    )
                    .all()
                )

            elif type == "angle":
                data = (
                    session
                    .query(Endf_ANGLE_Data)
                    .filter(
                        Endf_ANGLE_Data.reaction_id == r.reaction_id,
                    )
                    .all()
                )

            elif type == "residual" and r.projectile == "n":
                data = (
                    session
                    .query(Endf_N_Residual_Data)
                    .filter(
                        Endf_N_Residual_Data.reaction_id == r.reaction_id,
                    )
                    .all()
                )

            elif type == "residual" and r.projectile != "n":
                data = (
                    session
                    .query(Endf_Residual_Data)
                    .filter(
                        Endf_Residual_Data.reaction_id == r.reaction_id,
                    )
                    .all()
                )


            if len(data) == 0:
                print("no data")
                return -1

            else:
                return len(data)
        
        

    


def read_mt_json():
    if os.path.exists(MT_PATH_JSON):
        with open(MT_PATH_JSON) as map_file:
            return json.load(map_file)


def read_libs(projectiles=None):
    if not projectiles:
        projectiles = ["n"] #, "p", "g", "h", "t", "d", "a", "0"]

    mt_dict = read_mt_json()

    for p in projectiles:
        nuclides = [
            d
            for d in os.listdir(os.path.join(LIB_PATH, p))
            if os.path.isdir(os.path.join(LIB_PATH, p, d))
        ]
        nuclides = sorted(nuclides)
        # print(nuclides)
        processed = True
        for nuclide in nuclides:
            # print(p, nuclide)

            if nuclide == "La119":
                processed = False

            if processed:
                continue

            for lib in LIB_LIST:
                for type in ["xs", "fy", "residual", "angle"]:
                    files = []

                    if type == "fy":
                        path = os.path.join(
                            LIB_PATH, "FY", p, nuclide, lib, "tables", type.upper()
                        )

                    else:
                        path = os.path.join(LIB_PATH, p, nuclide, lib, "tables", type)


                    if os.path.exists(path):
                        files = [
                            os.path.join(path, f)
                            for f in os.listdir(path)
                            if os.path.isfile(os.path.join(path, f))
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

                    if not files:
                        continue

                    print("Processing: ", p, nuclide, type)

                    for file in files:
                        lib_df = pd.DataFrame()

                        mt = None
                        residual = None
                        iso = None

                        name = re.split("-|\.", os.path.basename(file))
                        ## xs:       ['n', 'Ac222m', 'MT018', 'tendl', '2021', 'txt']
                        ##           ['n', 'Ac222m', 'MT078', 'tendl', '2021', 'txt']
                        ##           ['n', 'Ac222m', 'MT062', 'tendl', '2021', 'txt'] 
                        ## residual: ['n', 'Ac222m', 'rp087204n', 'tendl', '2021', 'txt']
                        ## angle:    ['n', 'Ac222', 'MT056', 'Eang200', '000', 'tendl', '2021', 'txt']

                        if type == "xs":
                            # if name[2].endswith("m") or name[2].endswith("g"):
                            #     # In case if the file is for the production of metastable or ground explicitly given
                            #     continue

                            # else:
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


                        elif type == "angle":
                            mt = re.sub(r"\D", "", name[2])
                            residual = None

                        if check(p, nuclide, lib, type, mt, residual) == -1:
                            pass

                        elif check(p, nuclide, lib, type, mt, residual) > 0:
                            continue

                        ## process

                        reaction = Endf_Reactions()
                        reaction.evaluation = lib
                        reaction.type = type
                        reaction.target = nuclide
                        reaction.projectile = p
                        reaction.process = (
                            "INL"
                            if mt and mt == "004" and p == "n"
                            else "N"
                            if  mt and mt == "004" and p != "n"
                            else mt_dict[str(int(mt))]["sf3"]
                            if mt and mt_dict.get(str(int(mt)))
                            else "X" if type == "residual"
                            else None
                        )
                        
                        reaction.residual = residual
                        reaction.mf = 3 if type == "xs" else 8 if type == "fy" else 4 if type == "angle" else 10 if type == "residual" else None
                        reaction.mt = str(int(mt)) if mt else None
                        session.add(reaction)
                        session.commit()

                        reaction_id = reaction.reaction_id
                        connection = engine.connect()
                        
                        if type == "xs":
                            lib_df = create_libdf(file, reaction_id)
                            lib_df.to_sql(
                                "endf_xs_data",
                                connection,
                                index=False,
                                if_exists="append",
                            )

                        elif type == "residual" and p == "n":
                            lib_df = create_libdf(file, reaction_id)
                            lib_df.to_sql(
                                "endf_n_residual_data",
                                connection,
                                index=False,
                                if_exists="append",
                            )


                        elif type == "residual" and p != "n":
                            lib_df = create_libdf(file, reaction_id)
                            lib_df.to_sql(
                                "endf_residual_data",
                                connection,
                                index=False,
                                if_exists="append",
                            )

                        elif type == "fy":
                            lib_df = create_libdf_fy(file, reaction_id)
                            lib_df.to_sql(
                                "endf_fy_data",
                                connection,
                                index=False,
                                if_exists="append",
                            )

                        elif type == "angle":
                            lib_df = create_libdf_angle(file, reaction_id)
                            lib_df.to_sql(
                                "endf_angle_data",
                                connection,
                                index=False,
                                if_exists="append",
                            )

                        else:
                            continue

                        session.query(Endf_Reactions).filter(Endf_Reactions.reaction_id == reaction_id).update({"points" : len(lib_df.index)})
                        session.commit()
                        session.close()


    return lib_df




def create_libdf(libfile, reaction_id):
    try:
        lib_df = pd.read_csv(
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
        # if len(lib_df[lib_df["xslow"].notnull().all(1)]) == 0:
        lib_df["xslow"] *= 1e-3

        # if len(lib_df[lib_df["xsupp"].notnull().all(1)]) == 0:
        lib_df["xsupp"] *= 1e-3

    except:
        lib_df = pd.read_csv(
            libfile,
            sep="\s+",
            index_col=None,
            header=None,
            usecols=[0, 1],
            comment="#",
            names=["en_inc", "data"],
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
        en_inc = float(f.readlines()[16][29:41].strip())
        print(en_inc)

    lib_df = pd.read_csv(
        libfile,
        sep="\s+",
        index_col=None,
        header=None,
        usecols=[0, 1, 2, 3, 4],
        comment="#",
        names=["charge", "mass", "isomeric", "data", "ddata"],  # only in tendl.2021
    )

    lib_df["en_inc"] = en_inc
    lib_df["reaction_id"] = reaction_id

    lib_df = lib_df.reset_index()
    lib_df = lib_df.drop("index", axis=1)

    pd.options.display.float_format = '{:12.5e}'.format
    print(lib_df)
    return lib_df



def create_libdf_angle(libfile, reaction_id):
    with open(libfile, "r") as f:
        en_inc = float(f.readlines()[2][13:25].strip())

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

    return lib_df




def drop_tables():
    from src.endftables_sql.config import engine
    from src.endftables_sql.models import metadata

    for tbl in reversed(metadata.sorted_tables):
        engine.execute(tbl.delete())


if __name__ == "__main__":
    read_libs(projectiles)
    # try:
    #     read_libs()
    # except:
    #     pass
