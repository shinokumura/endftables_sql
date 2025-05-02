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
import sys


if len(sys.argv) > 1:
    type = list(sys.argv[1])
    projectiles = list(sys.argv[2])


FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
logging.basicConfig(filename="processed.log", level=logging.DEBUG, filemode="w")

from .models import Endf_Reactions, Endf_XS_Data, Endf_Residual_Data, Endf_N_Residual_Data, Endf_FY_Data, Endf_ANGLE_Data
from .config import session, connection, LIB_LIST, LIB_PATH

from .convert import read_mt_json, create_libdf
# connection = engine.connect()



def check_and_delete(p, nuclide, lib, type, mt):
    reac = (
        session
        .query(Endf_Reactions)
        .filter(
            Endf_Reactions.projectile == p,
            Endf_Reactions.target == nuclide,
            Endf_Reactions.evaluation == lib,
            Endf_Reactions.type == type,
            Endf_Reactions.mt == mt,
        )
        .all()
    )

    if len(reac) == 0:
        return len(reac)

    else:
        ## check the existence of data in the datatable
        print("Delete 'm, g, n' from tables: ", p, nuclide, lib, type, mt)
        for r in reac:
            # print(r.reaction_id, r.evaluation, r.target, r.projectile, r.process, r.residual, r.mt)

            if type == "xs":
                session.query(Endf_XS_Data).filter(
                        Endf_XS_Data.reaction_id == r.reaction_id,
                    ).delete()
                session.query(Endf_Reactions).filter(
                        Endf_Reactions.reaction_id == r.reaction_id,
                    ).delete()
                session.commit()
                print("deleted")

    return 



def replace_metastable(projectiles=None):
    if not projectiles:
        projectiles = ["n"]#, "p", "g", "h", "t", "d", "a", "0"]

    mt_dict = read_mt_json()

    for p in projectiles:
        nuclides = [
            d
            for d in os.listdir(os.path.join(LIB_PATH, p))
        ]
        nuclides = sorted(nuclides)
        processed = False

        for nuclide in nuclides:
            if processed:
                continue

            for lib in ["jendl5.0"]: # LIB_LIST:
                type = "xs"
                files = []
                table_path = os.path.join(LIB_PATH, p, nuclide, lib, "tables", type)

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
                    # print(f"{table_path} does not exist")
                    continue

                if not files:
                    print(f"no files in {table_path}")
                    continue

                    
                mt_list = {} ## create as dictionary to avoid duplication
                for file in files:
                    mt = None
                    residual = None
                    iso = None
                    name = re.split("-|\.", os.path.basename(file))
                    ## xs:       ['n', 'Ac222m', 'MT018', 'tendl', '2021', 'txt']
                    ##           ['n', 'Ac222m', 'MT078', 'tendl', '2021', 'txt']
                    ##           ['n', 'Ac222m', 'MT062', 'tendl', '2021', 'txt'] 
                    ## residual: ['n', 'Ac222m', 'rp087204n', 'tendl', '2021', 'txt']
                    ## angle:    ['n', 'Ac222', 'MT056', 'Eang200', '000', 'tendl', '2021', 'txt']

                    if name[2].endswith(('m','g','n','l')):
                        mt_list[ re.sub(r"\D", "", name[2]) ] = None
                    print(file, mt_list)
                mt_list = mt_list.keys()
                print(mt_list)

                replace_to_read_file = []
    #             for mt in mt_list:                  
    #                 ## delete rows from table
    #                 check_and_delete(p, nuclide, lib, type, mt)

    #                 ## files to replace
    #                 replace_to_read_file  += [f for f in files if mt in f and not re.split("-|\.", os.path.basename(f))[2].endswith(('m','g','n','l'))]

    #             # print(replace_to_read_file)


    #             for file in replace_to_read_file:
    #                 print("re-read", file)
    #                 residual = None

    #                 lib_df = pd.DataFrame()
    #                 reaction = Endf_Reactions()
    #                 reaction.evaluation = lib
    #                 reaction.type = type
    #                 reaction.target = nuclide
    #                 reaction.projectile = p
    #                 reaction.process = (
    #                     "INL"
    #                     if mt and mt == "004" and p == "n"
    #                     else "N"
    #                     if  mt and mt == "004" and p != "n"
    #                     else mt_dict[str(int(mt))]["sf3"]
    #                     if mt and mt_dict.get(str(int(mt)))
    #                     else "X" if type == "residual"
    #                     else None
    #                 )
                    
    #                 reaction.residual = residual
    #                 # reaction.en_inc = e_inc if e_inc else None   # only for angular distribution
    #                 reaction.mf = 3 if type == "xs" else 8 if type == "fy" else 4 if type == "angle" else 10 if type == "residual" else None
    #                 reaction.mt = str(int(mt)) if mt else None
    #                 session.add(reaction)
    #                 session.commit()

    #                 reaction_id = reaction.reaction_id

    #                 if type == "xs":
    #                     lib_df = create_libdf(file, reaction_id)
    #                     lib_df.to_sql(
    #                         "endf_xs_data",
    #                         connection,
    #                         index=False,
    #                         if_exists="append",
    #                     )

    #                 else:
    #                     continue

    #                 session.query(Endf_Reactions).filter(Endf_Reactions.reaction_id == reaction_id).update({"points" : len(lib_df.index)})
    #                 session.commit()
    #                 session.close()


    # return lib_df

if __name__ == "__main__":
    replace_metastable()