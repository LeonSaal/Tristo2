from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class WVG(Base):  # WW_Analysedaten_10-2021.xlsx
    __tablename__ = "wvg"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    wvgID = Column(String)
    supplied = Column(Integer)
    discharge_m3_p_d = Column(Integer)


class LAU_NUTS(Base):  # EU-27-LAU-2021-NUTS-2021.xlsx
    __tablename__ = "lau_nuts"
    id = Column(Integer, primary_key=True)
    LAU = Column(Integer)
    NUTS = Column(String)
    name = Column(String)
    population = Column(Integer)
    area_m2 = Column(Integer)


class WVG_LAU(Base):  # WW_Analysedaten_10-2021.xlsx
    __tablename__ = "wvg_lau"
    id = Column(Integer, primary_key=True)
    wvg = Column(Integer, ForeignKey(WVG.id))
    LAU = Column(Integer, ForeignKey(LAU_NUTS.LAU))


class GV3Q(Base):  # AuszugGV3QAktuell.xlsx
    __tablename__ = "gv3q"
    id = Column(Integer, primary_key=True)
    name = Column(String, ForeignKey(LAU_NUTS.name))
    ars = Column(Integer)


class GN250(Base):  # GN250.csv
    __tablename__ = "gn250"
    id = Column(Integer, primary_key=True)
    ars = Column(Integer, ForeignKey(GV3Q.ars))
    name = Column(String)


class Supplier(Base):
    __tablename__ = "supplier"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    url = Column(String)


class Response(Base):
    __tablename__ = "response"
    id = Column(Integer, primary_key=True)
    LAU = Column(Integer, ForeignKey(WVG_LAU.LAU))
    position = Column(Integer)
    link = Column(String)
    b_href = Column(String)
    time = Column(DateTime)
    status = Column(String)
    hash = Column(String)
    supplier = Column(Integer, ForeignKey(Supplier.id))
    postcode = Column(String)
    err_pdf = Column(Integer)
    err_other = Column(Integer)
    years = Column(String)
    init_link = Column(String)
    clicks = Column(Integer)


class File_Index(Base):
    __tablename__ = "file_index"
    hash = Column(String, ForeignKey(Response.hash))
    fname = Column(String)
    ext = Column(String)
    url = Column(String)
    hash2 = Column(String, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.fname}{self.ext} {self.hash!r} {self.hash2!r}"


class File_Info(Base):
    __tablename__ = "file_info"
    hash2 = Column(String, ForeignKey(File_Index.hash2), primary_key=True)
    status = Column(String)
    MB = Column(Float)
    pages = Column(Integer)
    date = Column(Integer)
    date_orig = Column(String)
    n_param = Column(Integer)
    districts = Column(String)
    LAUS = Column(String)
    data_basis = Column(String)
    analysis = Column(String)
    OMP = Column(String)


class File_Cleaned(Base):
    __tablename__ = "file_cleaned"
    hash2 = Column(String, ForeignKey(File_Index.hash2), primary_key=True)
    converter = Column(String)
    n_params = Column(Integer)
    status = Column(String)
    tabs_total = Column(Integer)
    tabs_dropped = Column(Integer)
    tabs_converted = Column(Integer)


class Param(Base):
    __tablename__ = "param"
    id = Column(Integer, primary_key=True)
    param = Column(String)
    alias = Column(String)
    regex = Column(String)
    limit = Column(String)
    unit = Column(String)
    origin = Column(String)
    category = Column(String)
    CAS = Column(String)


class Regex(Base):
    __tablename__ = "regex"
    id = Column(Integer, primary_key=True)
    regex = Column(String)
    category = Column(String)


class TableData(Base):
    __tablename__ = "tabledata"
    id = Column(Integer, primary_key=True)
    hash2 = Column(String, ForeignKey(File_Index.hash2))
    tab = Column(Integer)
    method = Column(Boolean)
    legal_lim = Column(Boolean)


class Data(Base):
    __tablename__ = "data"
    id = Column(Integer, primary_key=True)
    hash2 = Column(String, ForeignKey(File_Index.hash2))
    tab = Column(Integer, ForeignKey(TableData.id))
    col = Column(Integer)
    param = Column(String)
    param_id = Column(Integer, ForeignKey(Param.id))
    omitted_id = Column(Integer, ForeignKey(Regex.id))
    unit = Column(String)
    val = Column(String)
    val_num = Column(Float)
    category = Column(String)
    unit_factor = Column(Float)

class Mapping(Base):
    __tablename__ = "mapping"
    id = Column(Integer, primary_key=True)
    hash2 = Column(String, ForeignKey(File_Index.hash2))
    tab = Column(Integer, ForeignKey(TableData.id))
    col = Column(Integer)
    dist = Column(String)
    comm = Column(String)

class Unit(Base):
    __tablename__ = 'units'
    id = Column(Integer, primary_key=True)
    unit = Column(String)
    regex = Column(String)
    kind = Column(String)
    