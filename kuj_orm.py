import os
import sys
import csv
import json
from contextlib import contextmanager
import traceback

from sql_templates import CREATE_VIEWS

import sqlalchemy
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, Numeric
from sqlalchemy import func, and_, distinct, select
from sqlalchemy.schema import DDL
from sqlalchemy.orm import sessionmaker, relationship, backref, aliased, column_property
from sqlalchemy.types import PickleType

Base = declarative_base()

class Exp(Base):
    __tablename__ = 'experiment'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    ion_mode = Column(String)

    # backref to samples

class Mtab(Base):
    __tablename__ = 'metabolite'

    id = Column(Integer, primary_key=True)
    exp_id = Column(Integer, ForeignKey('experiment.id'))
    mz = Column(Numeric, index=True) # mass-to-charge ratio
    mzmin = Column(Numeric)
    mzmax = Column(Numeric)
    rt = Column(Numeric, index=True) # retention time (seconds)
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
        return '<Metabolite #%d %s %s %s (%s)>' % (self.id, self.exp.name, str(self.mz), str(self.rt), ms2)

class Sample(Base):
    __tablename__ = 'sample'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    control = Column(Integer)
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

def initialize_schema(engine):
    Base.metadata.create_all(engine)
    c = engine.connect()
    for cv in CREATE_VIEWS:
        c.execute(DDL(cv))

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

def etl(session, exp_name, df_path, mdf_path, ion_mode, log=None):
    if not log: # log progress
        log = lambda x: None
    exp = session.query(Exp).filter(Exp.name==exp_name).filter(Exp.ion_mode==ion_mode).first()
    if exp is None:
        exp = Exp(name=exp_name,ion_mode=ion_mode)
        session.add(exp)
    else:
        log("experiment %s has already been added to database, use 'remove %s' to remove it" % (exp_name, exp_name))
        return
    # first, do sample metadata for this experiment
    samples = {}
    ignored = 0
    required_sample_attrs = [FILE_NAME, CONTROL]
    with open(mdf_path,'rU') as cf:
        log('loading %s metadata from %s' % (exp_name, mdf_path))
        for d in csv.DictReader(cf):
            if IGNORE in d and d[IGNORE]=='1':
                ignored += 1
                continue # skip this sample
            name = d[FILE_NAME]
            control = int(d[CONTROL]) # 1 is True, 0 is False
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
    with open(df_path,'rU') as cf:
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
            n_samples = 0
            for cn,s in rest.items():
                if cn in samples:
                    n_samples += 1
                    sample = samples[cn]
                    intensity = float(s)
                    mi = MtabIntensity(mtab=m, sample=sample, intensity=intensity)
                    # FIXME use association proxy
                    session.add(mi)
            if n_samples == 0:
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

# util
def avoid_name_collisions(name,schema):
    n = 1
    newname = name
    while newname in schema:
        newname = '%s_%d' % (name, n)
        n += 1
    return newname

PPM_DIFF='ppm_diff'
RT_DIFF='rt_diff'
WITH_MS2='with_ms2'
EXCLUDE_CONTROLS='exclude_controls'
INT_OVER_CONTROLS='int_over_controls'
EXCLUDE_ATTRS='exclude_attrs'

def default_config():
    return {
        PPM_DIFF: 0.5,
        RT_DIFF: 30,
        WITH_MS2: False,
        EXCLUDE_CONTROLS: True,
        INT_OVER_CONTROLS: 0,
        EXCLUDE_ATTRS: set()
    }

def withms2_min(config):
    if config[WITH_MS2]:
        return 1
    else:
        return 0

class Db(object):
    def __init__(self, session, ion_mode, config=default_config()):
        self.session = session
        self.ion_mode = ion_mode
        self.config = config
    def remove_exp(self,exp):
        # http://stackoverflow.com/questions/19243964/python-sql-alchemy-cascade-delete
        theExp = self.session.query(Exp).filter(Exp.ion_mode==self.ion_mode).filter(Exp.name==exp).first()
        self.session.delete(theExp)
        self.session.commit()
    def all_attrs(self,exp=None):
        aa = {}
        for row in self.session.query(SampleAttr.name, SampleAttr.value).\
            join(Sample).join(Exp).filter(Exp.ion_mode==self.ion_mode).\
            order_by(SampleAttr.name, SampleAttr.value).\
            distinct():
            k, v = row
            if not k in aa:
                aa[k] = [v]
            else:
                aa[k] += [v]
        return aa
    def mtab_count(self,exp=None):
        q = self.session.query(func.count(Mtab.id)).filter(Mtab.exp.has(ion_mode=self.ion_mode))
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
    def matches_as_csv(self,pairs):
        exclude_controls = self.config[EXCLUDE_CONTROLS]
        int_over_controls = self.config[INT_OVER_CONTROLS]
        out_recs = []
        # fixed schema
        out_schema = [
            'mtab_exp', # source mtab experiment name
            'mtab_mz', # source mtab m/z
            'mtab_rt', # source mtab retention time
            'mtab_annotated', # source mtab annotation
            'match_exp', # matched mtab experiment name
            'match_mz', # matched mtab m/z
            'match_rt', # match mtab retention time
            'match_annotated', # match mtab annotation
            'sample', # sample / datafile containing matched mtab
            'intensity', # intensity of matched mtab in that sample
            'control' # is that sample a control sample
        ]
        n = 0
        for m, match in pairs:
            # exclude controls
            is_control = [mi.intensity>0 and mi.sample.control==1 for mi in match.intensities]
            if exclude_controls and any(is_control):
                continue
            # exclude matches not intense enough over controls
            aic = float(match.avg_int_controls) * int_over_controls
            is_le_aic = [float(mi.intensity) <= aic for mi in match.intensities]
            if all(is_le_aic):
                continue
            # now exclude based on sample attrs
            exclude = False
            for mi in match.intensities:
                for k,v in self.config[EXCLUDE_ATTRS].items():
                    for attr in mi.sample.attrs:
                        if attr.name==k and attr.value==v:
                            exclude = True
            if exclude:
                continue
            # this mtab will be included in results, count as a match
            n += 1
            for mi in match.intensities:
                if mi.intensity <= 0: # FIXME unnecessary if excluded from db
                    continue
                # populate fixed schema
                out_rec = {
                    'mtab_exp': m.exp.name,
                    'mtab_mz': m.mz,
                    'mtab_rt': m.rt,
                    'mtab_annotated': m.annotated,
                    'match_exp': match.exp.name,
                    'match_mz': match.mz,
                    'match_rt': match.rt,
                    'match_annotated': match.annotated,
                    'sample': mi.sample.name,
                    'intensity': mi.intensity,
                    'control': mi.sample.control
                }
                # now populate variable (per experiment) schema
                for attr in mi.sample.attrs:
                    # avoid collisions of attr names
                    attrname = avoid_name_collisions(attr.name, out_rec)
                    out_rec[attrname] = attr.value
                    if attrname not in out_schema: # keep track of all attributes we find
                        out_schema.append(attrname)
                out_recs.append(out_rec) # save record
        # now we have all the output records in hand
        # format the output records according to the accumulated union schema
        def outlines():
            yield ','.join(out_schema)
            for rec in out_recs:
                out_row = [rec.get(k,'') for k in out_schema]
                yield ','.join(map(str,out_row)) # FIXME format numbers better
        return list(outlines()), n
    def ctest(self):
        mtab = self.mtab_random()
        print mtab
        for row in self.session.query(Mtab, Sample.control, func.avg(MtabIntensity.intensity)).\
            join(MtabIntensity).join(Sample).\
            filter(Mtab.id==mtab.id).\
            group_by(Mtab, Sample.control):
            print row
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
def DomDb(sessionfactory,ion_mode,config=default_config()):
    try:
        session = sessionfactory()
        yield Db(session, ion_mode, config)
    except:
        print 'Error running command:'
        traceback.print_exc(file=sys.stdout)
    finally:
        session.close()

