####################################################################
#
# This file is part of exfor-parser.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Disclaimer: The code is still under developments and not ready
#             to use. It has been made public to share the progress
#             among collaborators.
# Contact:    nds.contact-point@iaea.org
#
####################################################################
import re

# from .config import session_lib
from endftables_sql.config import engines, session_lib, LIB_LIST
from .models import Endf_Reactions
from sqlalchemy import update, insert, Table, select, and_, or_
from endftables_sql.submodules.utilities.reaction import mt_to_discretelevel
from endftables_sql.scripts.convert import metadata, glob_files_from_liball, glob_nuclides_from_liball, extract_info_from_fn, check, process_one_file
import logging
logging.basicConfig(filename=f"check.log", level=logging.ERROR, filemode="w")

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def maintenance_a():
    # reactions = Endf_Reactions.query.filter_by(type='residual').first()
    reactions = session_lib.query(Endf_Reactions).filter(Endf_Reactions.type =='residual').all()
    # reactions = session_lib().query(Endf_Reactions).filter(Endf_Reactions.type =='residual').limit(100000).all()
    # reactions = session_lib().query(Endf_Reactions).filter(Endf_Reactions.type =='residual').first()

    # i = 0
    for r in reactions:
        nuclides = re.split(r'(\d+)', r.residual)
        if len(nuclides[1]) == 2:
            # i += 1
            print( f"{nuclides[0]}{nuclides[1].zfill(3)}{nuclides[2]}" )
            r.residual = f"{nuclides[0]}{nuclides[1].zfill(3)}{nuclides[2]}"

        # if i > 5:
        #     break
        
        # break

    # session_lib.flush()
    session_lib.commit()



def update_process():
    reactions = session_lib.query(Endf_Reactions.projectile, Endf_Reactions.process, Endf_Reactions.mt).filter(Endf_Reactions.type.in_(tuple(["angle", "xs"]))).distinct()


    for r in reactions:
        projectile, process, mt = r
        mt = str(mt) #.zfill(3)
        process_fix =  mt_to_discretelevel(projectile, mt)

        print(projectile, process, process_fix, mt)
        session_lib.query(Endf_Reactions).filter(Endf_Reactions.mt == mt).update({"process" : process_fix})
        session_lib.commit()
    session_lib.close()


def check_omissions(run_type):
    """
    This is for Cd000 and Fe056 where the shell script run fail because of too many files.
    """
    projectiles = ["n"] #["a", "d", "g", "h", "p", "t", "n"]

    run_type = None
    nfl = "A-Z"

    for projectile in projectiles:
        reaction_list = []
        obs_types, nuclides = glob_nuclides_from_liball(run_type, projectile, nfl)
        # print(obs_types, nuclides)
        for nuclide in nuclides:
        # for nuclide in ["Fe056", "Cd000"]:
            print(projectile, nuclide) 
            for lib in LIB_LIST:
                connection = engines["endftables"].connect()
                endf_reactions = Table("endf_reactions", metadata, autoload_with=connection)   
                for obs_type in obs_types:
                    files = glob_files_from_liball(projectile, nuclide, lib, obs_type)

                    if not files:
                        continue

                    for file in files:
                        mt, residual, en_inc = extract_info_from_fn(file, obs_type)
                        if mt:
                            count = check(connection, endf_reactions, projectile, nuclide, lib, obs_type, en_inc, mt, residual)

                            if count == -1:
                                print("not found:", projectile, nuclide, lib, obs_type, en_inc, mt, residual, file)
                                logging.error(f"Not found in {projectile}, {nuclide}, {lib}, {obs_type}, {en_inc}, {mt}, {residual}\n{file}", exc_info=True)
                            if count > 0:
                                continue
                        else:
                            continue
                connection.close()



def add_unloaded_files():

    """ unloaded.txt is the output of @check_omissions and the following one liner
    $ sed "s/ERROR\:root\:Not found in //g" check.log | grep -v "NoneType" > a
    n, Cd000, tendl.2023, xs, None, 051, None
    n, Cd000, tendl.2023, xs, None, 044, None
    n, Cd000, tendl.2023, xs, None, 204, None
    n, Cd000, tendl.2023, xs, None, 028, None
    n, Cd000, tendl.2023, xs, None, 032, None
    n, Cd000, tendl.2023, xs, None, 203, None
    n, Cd000, tendl.2023, xs, None, 202, None
    n, Cd000, tendl.2023, xs, None, 033, None
    """

    with open("unloaded.log", "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    for i in range(0, len(lines), 2):
        data_line = lines[i]
        file_line = lines[i + 1] if i + 1 < len(lines) else None

        projectile, nuclide, lib, obs_type, en_inc, mt, residual = [x.strip() if x != 'None' else None for x in data_line.split(",")]
        # print(projectile, nuclide, lib, obs_type, en_inc, mt, residual)

        process_one_file(projectile, nuclide, lib, obs_type, file_line)

add_unloaded_files()

