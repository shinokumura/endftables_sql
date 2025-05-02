
import os
import argparse
import pathlib
import logging
logging.basicConfig(filename="process.log", level=logging.ERROR, filemode="w")

from endftables_sql.models import create_all
from endftables_sql.convert import read_libs

PARTICLES = ["a", "d", "g", "h", "n", "p", "t"]


def cli():
    parser = argparse.ArgumentParser(
        prog="Convert EDNFTABLES to SQLite Database",
        add_help=False,
    )

    parser.add_argument(
        "-i",
        "--init",
        help="Create and initialize the SQLite Database",
    )

    parser.add_argument(
        "-conv",
        "--convert",

        choices=PARTICLES,
        help="Convert EDNFTABLES to SQLite Database",
    )


    args = parser.parse_args()

    if args.init:
        create_all()

    if args.convert:
        projectile = args.convert
        read_libs(projectile)


        # run = "fy"# "non_fy" # or "fy"
        # if run != "fy":
        #     projectiles = ["n", "p", "g", "h", "t", "d", "a"]
        # else:
        #     projectiles = ["n", "0"]

