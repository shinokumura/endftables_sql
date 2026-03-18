import sqlalchemy as db
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


# =========================
# Engine-independent API
# =========================

resonancetable_data = db.Table(
    "resonancetable_data",
    metadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("z", db.Integer),
    db.Column("a", db.Integer),
    db.Column("liso", db.Integer),
    db.Column("nuclide", db.String),
    db.Column("data_type", db.String),   # "macs", "thermal", "resonance_param"
    db.Column("quantity", db.String),    # "ng", "D0", "S0", "el", "nf", etc.
    db.Column("source", db.String),      # "Kadonis", "Mughabghab-2018", "selected", etc.
    db.Column("value", db.Float),
    db.Column("dvalue", db.Float),
    db.Column("rel_dev_comp", db.Float),
    db.Column("rel_dev_ndl", db.Float),
    db.Column("rel_dev_exfor", db.Float),
    db.Column("rel_dev_all", db.Float),
    db.Column("n_exper", db.Integer),
    db.Column("spectrum", db.String),    # "MXW" or None
)


def create_all(engine):
    """
    Create all tables using provided engine.
    """
    metadata.create_all(bind=engine)


def drop_all(engine):
    """
    Drop all tables using provided engine.
    """
    metadata.drop_all(bind=engine)