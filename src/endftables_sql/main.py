
import os
import argparse
import logging
logging.basicConfig(filename=f"process.log", level=logging.ERROR, filemode="w")
from multiprocessing import Process, Pool

from endftables_sql.scripts.models import create_all
from endftables_sql.scripts.convert import process_all

PARTICLES = ["a", "d", "g", "h", "n", "p", "t", "fy"]


def cli():
    parser = argparse.ArgumentParser(
        prog="Convert EDNFTABLES to SQLite Database",
        add_help=False,
    )

    parser.add_argument(
        "-i",
        "--init",
        help="Create and initialize the SQLite Database",
        action='store_true'
    )

    parser.add_argument(
        "-c",
        "--convert",
        choices=PARTICLES,
        help="Convert EDNFTABLES to SQLite Database",
    )


    args = parser.parse_args()
    print(args)
    

    if args.init:
        create_all()

    if args.convert == "fy":
        projectile = args.convert
        process_all(type="fy", projectile="n")
        process_all(type="fy", projectile="0")

    if args.convert:
        projectile = args.convert
        
        if projectile == "n":
            # inp = ["A-D", "E-J", "K-O", "P-S", "T-Z"]  # Input list
            # args = [("", "n", i) for i in inp]

            # with Pool(processes=4) as pool:
            #     r = pool.starmap(process_all, args)  # Parallel execution
            process_all(type=None, projectile='n', nfl="A-Z")

        else:
            process_all(type=None, projectile=projectile, nfl="A-Z")


