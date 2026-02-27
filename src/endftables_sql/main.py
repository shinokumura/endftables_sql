import argparse
from endftables_sql.scripts.models import create_all
from endftables_sql.scripts.convert import process_all

PARTICLES = ["a", "d", "g", "h",  "p", "t", "n", "fy"]


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

    if args.convert:
        projectile = args.convert

    if args.convert == "fy":
        process_all(run_type="fy", projectile="n", nfl="A-Z")
        process_all(run_type="fy", projectile="0", nfl="A-Z")

    elif args.convert == "resonance":
        projectile = args.convert


    elif args.convert in ["n", "a", "d", "g", "h", "p", "t"]:
        process_all(run_type=None, projectile=projectile, nfl="A-Z")


if __name__ == "__main__":
    cli()