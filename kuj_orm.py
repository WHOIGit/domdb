import os
import sys
import csv
import json
from contextlib import contextmanager
import traceback

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
    isotopes = Column(String)
    adduct = Column(String)
    pcgroup = Column(Integer)
    withMS2 = Column(Integer, default=0)
    annotated = Column(String, default='')

    exp = relationship(Exp, backref=backref('mtabs', cascade='all,delete-orphan'))

    def __repr__(self):
        if self.withMS2==1:
            ms2 = 'with MS2'
        else:
            ms2 = 'no MS2'
        return '<Metabolite %s %s %s (%s)>' % (self.exp.name, str(self.mz), str(self.rt), ms2)

class Sample(Base):
    __tablename__ = 'sample'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    control = Column(Boolean)
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

COMMON_FIELDS=set([
    'mz',
    'mzmin',
    'mzmax',
    'rt',
    'rtmin',
    'rtmax',
    'isotopes',
    'adduct',
    'pcgroup',
    'withMS2',
    'annotated'
])
IGNORE='ignore'
CONTROL='control'
FILE_NAME='File.Name'

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
    ignored = 0
    required_sample_attrs = [FILE_NAME, CONTROL]
    with open(mdf_path) as cf:
        log('loading %s metadata from %s' % (exp_name, mdf_path))
        for d in csv.DictReader(cf):
            if IGNORE in d and d[IGNORE]=='1':
                ignored += 1
                continue # skip this sample
            name = d[FILE_NAME]
            control = d[CONTROL]==1
            sample = Sample(name=name,control=control)
            samples[name] = sample
            sample.exp = exp
            session.add(sample)
            # now add addrs
            for k,v in d.items():
                if k not in required_sample_attrs:
                    attr = SampleAttr(sample=sample, name=k, value=v)
                    session.add(attr) # FIXME use association proxy
    log('%d total samples loaded, %d ignored' % (len(samples), ignored))
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
            samples_found = False
            for cn,s in rest.items():
                if cn in samples:
                    samples_found = True
                    intensity = float(s)
                    if intensity != 0:
                        mi = MtabIntensity(mtab=m, sample=samples[cn], intensity=intensity)
                        # FIXME use association proxy
                        session.add(mi)
            if not samples_found:
                log('ERROR: all samples missing from metabolite record, wrong metadata file?')
                log('metabolite record columns (in no particular order): %s' % keys)
                session.rollback()
                return
            # add to session
            session.add(m)
            n += 1
            if n % 1000 == 0:
                log('loaded %d metabolites so far' % n)
                session.commit()
    session.commit()
    log('loaded %d total metabolites' % n)

PPM_DIFF='ppm_diff'
RT_DIFF='rt_diff'
WITH_MS2='with_ms2'

def default_config():
    return {
        PPM_DIFF: 0.5,
        RT_DIFF: 30,
        WITH_MS2: False
    }

def withms2_min(config):
    if config[WITH_MS2]:
        return 1
    else:
        return 0

class Db(object):
    def __init__(self, session, config=default_config()):
        self.session = session
        self.config = config
    def remove_exp(self,exp):
        # FIXME cascading ORM delete should make this unnecessary
        self.session.query(Mtab).filter(Mtab.exp.has(name=exp)).delete(synchronize_session='fetch')
        self.session.commit()
        self.session.query(Sample).filter(Sample.exp.has(name=exp)).delete(synchronize_session='fetch')
        self.session.commit()
        self.session.query(Exp).filter(Exp.name==exp).delete(synchronize_session='fetch')
        self.session.commit()
    def mtab_count(self,exp=None):
        q = self.session.query(func.count(Mtab.id))
        if exp is not None:
            q = q.filter(Mtab.exp.has(name=exp))
        return q.first()[0]
    def match_all_from(self,exp):
        m_alias = aliased(Mtab)
        for row in self.session.query(Mtab, m_alias).\
            filter(Mtab.exp.has(name=exp)).\
            filter(Mtab.withMS2 >= withms2_min(self.config)).\
            join((m_alias, and_(Mtab.id != m_alias.id, Mtab.exp_id != m_alias.exp_id))).\
            filter(m_alias.withMS2 >= withms2_min(self.config)).\
            filter(func.abs(Mtab.rt - m_alias.rt) <= self.config[RT_DIFF]).\
            filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= self.config[PPM_DIFF]).\
            join((Exp, Exp.id==m_alias.exp_id)).\
            order_by(Mtab.mz, Exp.name).\
            all():
            yield row
    def match_all(self):
        m_alias = aliased(Mtab)
        for row in self.session.query(Mtab, m_alias).\
            filter(Mtab.withMS2 >= withms2_min(self.config)).\
            join((m_alias, Mtab.id != m_alias.id)).\
            filter(m_alias.withMS2 >= withms2_min(self.config)).\
            filter(func.abs(Mtab.rt - m_alias.rt) <= self.config[RT_DIFF]).\
            filter(func.abs(1e6 * (Mtab.mz - m_alias.mz) / m_alias.mz) <= self.config[PPM_DIFF]).\
            all():
            yield row
    def match_one(self,m):
        for row in self.session.query(Mtab).\
            filter(Mtab.id != m.id).\
            filter(Mtab.withMS2 >= withms2_min(self.config)).\
            filter(func.abs(Mtab.rt - m.rt) <= self.config[RT_DIFF]).\
            filter(func.abs(1e6 * (Mtab.mz - m.mz) / m.mz) <= self.config[PPM_DIFF]).\
            all():
            yield row
    def mtab_search(self,mz,rt):
        for m in self.session.query(Mtab).\
            filter(Mtab.withMS2 >= withms2_min(self.config)).\
            filter(func.abs(1e6 * (mz - Mtab.mz) / Mtab.mz) <= self.config[PPM_DIFF]).\
            filter(func.abs(rt - Mtab.rt) <= self.config[RT_DIFF]):
            yield m
    def mtab_random(self):
        return self.session.query(Mtab).order_by(func.random()).limit(1)[0]
    def mtab_dist(self,n=1000):
        pdf = {}
        for i in range(n):
            mtab = self.mtab_random()
            n_ms = len(list(self.match_one(mtab))) + 1
            if n_ms > 1:
                print mtab, n_ms
            if n_ms not in pdf:
                pdf[n_ms] = 1
            else:
                pdf[n_ms] = pdf[n_ms] + 1
        return pdf

@contextmanager
def DomDb(sessionfactory,config=default_config()):
    try:
        session = sessionfactory()
        yield Db(session, config)
    except:
        print 'Error running command:'
        traceback.print_exc(file=sys.stdout)
    finally:
        session.close()

