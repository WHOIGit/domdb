import os
import csv
import json

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, Numeric, func, and_
from sqlalchemy.orm import sessionmaker, relationship, backref, aliased
from sqlalchemy.types import PickleType

Base = declarative_base()

class Exp(Base):
    __tablename__ = 'experiment'

    id = Column(Integer, primary_key=True)
    name = Column(String)

class Mtab(Base):
    __tablename__ = 'metabolite'

    id = Column(Integer, primary_key=True)
    exp_id = Column(Integer, ForeignKey('experiment.id'))
    mz = Column(Numeric) # mass-to-charge ratio
    mzmin = Column(Numeric)
    mzmax = Column(Numeric)
    rt = Column(Numeric) # retention time (seconds)
    rtmin = Column(Numeric)
    rtmax = Column(Numeric)

    exp = relationship(Exp)

    def __repr__(self):
        return '<Metabolite %s %s %s>' % (self.exp.name, str(self.mz), str(self.rt))

class Sample(Base):
    __tablename__ = 'sample'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    exp_id = Column(Integer, ForeignKey('experiment.id'))

    exp = relationship(Exp, backref=backref('samples', cascade='all,delete-orphan'))

class SampleAttr(Base):
    __tablename__ = 'sample_attr'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey('sample.id'))
    name = Column(String)
    value = Column(String)

    sample = relationship(Sample, backref=backref('attrs', cascade='all,delete-orphan'))

class MtabIntensity(Base):
    __tablename__ = 'intensity'

    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey('sample.id'))
    mtab_id = Column(Integer, ForeignKey('metabolite.id'))
    intensity = Column(Numeric)

    sample = relationship(Sample)
    mtab = relationship(Mtab)

def get_sqlite_engine(delete=True):
    # first, toast db
    DB_FILE = 'kuj2.db'
    if delete and os.path.exists(DB_FILE):
        try:
            os.remove(DB_FILE)
        except:
            print 'unable to delete %s' % DB_FILE
            raise
    return sqlalchemy.create_engine('sqlite:///%s' % DB_FILE)

COMMON_FIELDS=set(['mz','mzmin','mzmax','rt','rtmin','rtmax'])

def etl(session, exp_name, df_path, mdf_path):
    exp = session.query(Exp).filter(Exp.name==exp_name).first()
    if exp is None:
        exp = Exp(name=exp_name)
        session.add(exp)
    # first, do sample metadata for this experiment
    samples = {}
    with open(mdf_path) as cf:
        for d in csv.DictReader(cf):
            name = d['File.Name']
            sample = Sample(name=name)
            samples[name] = sample
            sample.exp = exp
            session.add(sample)
            # now add addrs
            for k,v in d.items():
                if k != 'File.Name':
                    attr = SampleAttr(sample=sample, name=k, value=v)
                    session.add(attr) # FIXME use association proxy
    # now add metabolite data
    with open(df_path) as cf:
        for d in csv.DictReader(cf):
            # subset the fields
            keys = set(d.keys())
            rest_keys = keys.difference(COMMON_FIELDS)
            md = dict((k,d[k]) for k in COMMON_FIELDS)
            md['exp'] = exp
            # construct ORM object for metabolite
            m = Mtab(**md)
            # now record mtab intensity per sample
            rest = dict((k,d[k]) for k in rest_keys if k)
            for cn,s in rest.items():
                if cn in samples:
                    if s != '0':
                        print cn,s
                        # FIXME use association proxy
                        session.add(MtabIntensity(mtab=m, sample=samples[cn], intensity=s))
            # add to session
            session.add(m)
    session.commit()

def etl_all(session):
    exp_data = {
        'tps4': 'data/Tps4_pos_2014.05.23.csv',
        'tps6': 'data/Tps6_pos_2014.05.23.csv'
    }
    exp_metadata = {
        'tps4': 'metadata/Tps4_metadata.csv',
        'tps6': 'metadata/Tps6_metadata.csv'
    }
    for e in exp_data.keys():
        data = exp_data[e]
        metadata = exp_metadata[e]
        etl(session, e, data, metadata)
    # now commit
    session.commit()

def match_all_from(session,exp,ppm_diff=0.5,rt_diff=30):
    m_alias = aliased(Mtab)
    for m in session.query(Mtab, m_alias).\
        join((m_alias, and_(Mtab.id != m_alias.id, Mtab.exp_id != m_alias.exp_id))).\
        filter(Mtab.exp.name == exp).\
        filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= ppm_diff).\
        filter(func.abs(Mtab.rt - m_alias.rt) <= rt_diff).\
        order_by(Mtab.mz, m_alias.exp.name):
        yield m
    
def match_all(session,ppm_diff=0.5,rt_diff=30):
    m_alias = aliased(Mtab)
    for m in session.query(Mtab, m_alias).\
        join((m_alias, Mtab.id != m_alias.id)).\
        filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= ppm_diff).\
        filter(func.abs(Mtab.rt - m_alias.rt) <= rt_diff):
        yield m

def match_one(session,m,ppm_diff=0.5,rt_diff=30):
    for m in session.query(Mtab).\
        filter(Mtab.id != m.id).\
        filter(func.abs(1e6 * (Mtab.mz - m.mz) / m.mz) <= ppm_diff).\
        filter(func.abs(Mtab.rt - m.rt) <= rt_diff):
        yield m

def mtab_search(session,mz,rt,ppm_diff=0.5,rt_diff=30):
    for m in session.query(Mtab).\
        filter(func.abs(1e6 * (mz - Mtab.mz) / Mtab.mz) <= ppm_diff).\
        filter(func.abs(rt - Mtab.rt) <= rt_diff):
        yield m

def mtab_random(session):
    return session.query(Mtab).order_by(func.random()).limit(1)[0]

DELETE=True

if __name__=='__main__':
    engine = get_sqlite_engine(delete=DELETE)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    if DELETE: # we deleted data, now need to load it
        print 'Loading data...'
        etl_all(session)
    # now count how many entries we have
    n = session.query(func.count(Mtab.id)).first()[0]
    print '%d metabolites loaded' % n
    # now pick four metabolites at random
    for m in session.query(Mtab).order_by(func.random()).limit(10):
        print 'Looking for matches for %s' % m
        # for each one, find matching ones
        for match in match_one(session,m):
            print 'Match found: %s' % match
    # now get metadata for random metabolites
    for m in session.query(Mtab).order_by(func.random()).limit(2):
        print 'Info about %s' % m
        for mi in session.query(MtabIntensity).\
            filter(MtabIntensity.mtab_id==m.id):
            print '%s=%f {' % (mi.sample.name, mi.intensity)
            for attr in mi.sample.attrs:
                print '"%s": "%s"' % (attr.name, attr.value)
            print '}'
