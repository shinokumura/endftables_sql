import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from endftables_sql.config import engines

Base = declarative_base()
metadata = db.MetaData()


class Endf_Reactions(Base):
    __tablename__ = "endf_reactions"
    reaction_id = db.Column(
        db.Integer, primary_key=True, autoincrement=True
    )
    evaluation = db.Column(db.String)
    obs_type = db.Column(db.String)
    target = db.Column(db.String)
    projectile = db.Column(db.String)
    process = db.Column(db.String)
    residual = db.Column(db.String)
    en_inc = db.Column(db.Float) # For FY and angular distribution
    points = db.Column(db.Integer)
    mf = db.Column(db.Integer)
    mt = db.Column(db.Integer)


class Endf_XS_Data(Base):
    __tablename__ = "endf_xs_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reaction_id = db.Column(db.Integer)
    en_inc = db.Column(db.Float)
    data = db.Column(db.Float)
    xslow = db.Column(db.Float)
    xsupp = db.Column(db.Float)


class Endf_Residual_Data(Base):
    __tablename__ = "endf_residual_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reaction_id = db.Column(db.Integer)
    en_inc = db.Column(db.Float)
    data = db.Column(db.Float)
    xslow = db.Column(db.Float)
    xsupp = db.Column(db.Float)


class Endf_N_Residual_Data(Base):
    __tablename__ = "endf_n_residual_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reaction_id = db.Column(db.Integer)
    en_inc = db.Column(db.Float)
    data = db.Column(db.Float)
    xslow = db.Column(db.Float)
    xsupp = db.Column(db.Float)


class Endf_FY_Data(Base):
    __tablename__ = "endf_fy_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reaction_id = db.Column(db.Integer)
    en_inc = db.Column(db.Float)
    mass = db.Column(db.Float)
    charge = db.Column(db.Float)
    isomeric = db.Column(db.Float)
    data = db.Column(db.Float)
    ddata = db.Column(db.Float)



class Endf_Angle_Data(Base):
    __tablename__ = "endf_angle_data"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reaction_id = db.Column(db.Integer)
    en_inc = db.Column(db.Float)
    angle = db.Column(db.Float)
    data = db.Column(db.Float)
    ddata = db.Column(db.Float)
    frame = db.Column(db.String)




def create_all():
    Base.metadata.create_all(bind=engines["endftables"])


if __name__ == "__main__":
    create_all()

