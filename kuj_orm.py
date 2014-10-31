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

    # backref to samples

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

    exp = relationship(Exp, backref=backref('mtabs', cascade='all,delete-orphan'))

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

    sample = relationship(Sample, backref=backref('intensities', cascade='all,delete-orphan'))
    mtab = relationship(Mtab, backref=backref('intensities', cascade='all,delete-orphan'))

COMMON_FIELDS=set(['mz','mzmin','mzmax','rt','rtmin','rtmax'])

def etl(session, exp_name, df_path, mdf_path, log=None):
    if not log: # log progress
        log = lambda x: None
    exp = session.query(Exp).filter(Exp.name==exp_name).first()
    if exp is None:
        exp = Exp(name=exp_name)
        session.add(exp)
    else:
        log("experiment %s has already been added to database, use 'remove %s' to remove it" % (exp_name, exp_name))
        return
    # first, do sample metadata for this experiment
    samples = {}
    with open(mdf_path) as cf:
        log('loading %s metadata from %s' % (exp_name, mdf_path))
        n = 0
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
            n += 1
            if n % 100 == 0:
                log('loaded %d samples so far' % n)
                session.commit()
    session.commit()
    log('%d total samples loaded' % n)
    n = 0
    # now add metabolite data
    with open(df_path) as cf:
        log('loading %s metabolite data from %s' % (exp_name, df_path))
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
                        # FIXME use association proxy
                        session.add(MtabIntensity(mtab=m, sample=samples[cn], intensity=s))
            # add to session
            session.add(m)
            n += 1
            if n % 1000 == 0:
                log('loaded %d metabolites so far' % n)
                session.commit()
    session.commit()
    log('loaded %d total metabolites' % n)

def remove_exp(session,exp):
    # FIXME cascading ORM delete should make this unnecessary
    session.query(Mtab).filter(Mtab.exp.has(name=exp)).delete(synchronize_session='fetch')
    session.commit()
    session.query(Sample).filter(Sample.exp.has(name=exp)).delete(synchronize_session='fetch')
    session.commit()
    session.query(Exp).filter(Exp.name==exp).delete(synchronize_session='fetch')
    session.commit()
    
def match_all_from(session,exp,ppm_diff=0.5,rt_diff=30):
    m_alias = aliased(Mtab)
    for row in session.query(Mtab, m_alias).\
        filter(Mtab.exp.has(name=exp)).\
        join((m_alias, and_(Mtab.id != m_alias.id, Mtab.exp_id != m_alias.exp_id))).\
        filter(func.abs(Mtab.rt - m_alias.rt) <= rt_diff).\
        filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= ppm_diff).\
        join((Exp, Exp.id==m_alias.exp_id)).\
        order_by(Mtab.mz, Exp.name).\
        all():
        yield row

def match_all(session,ppm_diff=0.5,rt_diff=30):
    m_alias = aliased(Mtab)
    for row in session.query(Mtab, m_alias).\
        join((m_alias, Mtab.id != m_alias.id)).\
        filter(func.abs(Mtab.rt - m_alias.rt) <= rt_diff).\
        filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= ppm_diff).\
        all():
        yield row

def match_one(session,m,ppm_diff=0.5,rt_diff=30):
    for row in session.query(Mtab).\
        filter(Mtab.id != m.id).\
        filter(func.abs(Mtab.rt - m.rt) <= rt_diff).\
        filter(func.abs(1e6 * (Mtab.mz - m.mz) / m.mz) <= ppm_diff).\
        all():
        yield row

def mtab_search(session,mz,rt,ppm_diff=0.5,rt_diff=30):
    for m in session.query(Mtab).\
        filter(func.abs(1e6 * (mz - Mtab.mz) / Mtab.mz) <= ppm_diff).\
        filter(func.abs(rt - Mtab.rt) <= rt_diff):
        yield m

def mtab_random(session):
    return session.query(Mtab).order_by(func.random()).limit(1)[0]
