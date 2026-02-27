import sqlalchemy as db
from sqlalchemy import UniqueConstraint
from endftables_sql.config import engines

metadata = db.MetaData()

endf_reactions = db.Table(
    "endf_reactions",
    metadata,
    db.Column("reaction_id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("evaluation", db.String),
    db.Column("obs_type", db.String),
    db.Column("target", db.String),
    db.Column("projectile", db.String),
    db.Column("process", db.String),
    db.Column("residual", db.String),
    db.Column("en_inc", db.Float),
    db.Column("points", db.Integer),
    db.Column("mf", db.Integer),
    db.Column("mt", db.Integer),

    # UniqueConstraint(
    #     "evaluation",
    #     "obs_type",
    #     "target",
    #     "projectile",
    #     "en_inc",
    #     "mt",
    #     "residual",
    #     name="uq_endf_reaction"
    # ),
)

endf_xs_data = db.Table(
    "endf_xs_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("reaction_id", db.Integer),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_residual_data = db.Table(
    "endf_residual_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("reaction_id", db.Integer),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_n_residual_data = db.Table(
    "endf_n_residual_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("reaction_id", db.Integer),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_fy_data = db.Table(
    "endf_fy_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("reaction_id", db.Integer),
    db.Column("en_inc", db.Float),
    db.Column("mass", db.Float),
    db.Column("charge", db.Float),
    db.Column("isomeric", db.Float),
    db.Column("data", db.Float),
    db.Column("ddata", db.Float),
)

endf_angle_data = db.Table(
    "endf_angle_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("reaction_id", db.Integer),
    db.Column("en_inc", db.Float),
    db.Column("angle", db.Float),
    db.Column("data", db.Float),
    db.Column("ddata", db.Float),
    db.Column("frame", db.String),
)



def create_all():
    metadata.create_all(bind=engines["endftables"])

if __name__ == "__main__":
    create_all()
