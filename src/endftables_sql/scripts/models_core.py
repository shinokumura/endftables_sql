import sqlalchemy as db
from endftables_sql.config import engines

metadata = db.MetaData()

endf_reactions = db.Table(
    "endf_reactions",
    metadata,
    db.Column("reaction_id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("evaluation", db.String, index=True),
    db.Column("type", db.String, index=True),
    db.Column("target", db.String, index=True),
    db.Column("projectile", db.String),
    db.Column("process", db.String, index=True),
    db.Column("residual", db.String, index=True),
    db.Column("en_inc", db.Float, index=True),
    db.Column("points", db.Integer),
    db.Column("mf", db.Integer),
    db.Column("mt", db.Integer, index=True),
)

endf_xs_data = db.Table(
    "endf_xs_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("reaction_id", db.Integer, index=True),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_residual_data = db.Table(
    "endf_residual_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("reaction_id", db.Integer, index=True),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_n_residual_data = db.Table(
    "endf_n_residual_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("reaction_id", db.Integer, index=True),
    db.Column("en_inc", db.Float),
    db.Column("data", db.Float),
    db.Column("xslow", db.Float),
    db.Column("xsupp", db.Float),
)

endf_fy_data = db.Table(
    "endf_fy_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("reaction_id", db.Integer, index=True),
    db.Column("en_inc", db.Float, index=True),
    db.Column("mass", db.Float, index=True),
    db.Column("charge", db.Float, index=True),
    db.Column("isomeric", db.Float),
    db.Column("data", db.Float),
    db.Column("ddata", db.Float),
)

endf_angle_data = db.Table(
    "endf_angle_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, index=True),
    db.Column("reaction_id", db.Integer, index=True),
    db.Column("en_inc", db.Float, index=True),
    db.Column("angle", db.Float, index=True),
    db.Column("data", db.Float),
    db.Column("ddata", db.Float),
    db.Column("frame", db.String),
)

def create_all():
    metadata.create_all(bind=engines["endftables"])

if __name__ == "__main__":
    create_all()
