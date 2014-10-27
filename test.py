import os

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from kuj_orm import Base, Mtab, MtabIntensity
from kuj_orm import etl, match_one, mtab_random, remove_exp

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

def etl_examples(session):
    exp_data = {
#        'tps4': 'data/Tps4_pos_2014.05.23.csv',
#        'tps6': 'data/Tps6_pos_2014.05.23.csv'
        'deepdom': 'LCdata/DeepDOMtraps_pos_aligned.2014.05.05_v1.csv',
        'ventdom': 'LCdata/ventDOM_pos_aligned.2014.05.20_v1.csv'
    }
    exp_metadata = {
#        'tps4': 'metadata/Tps4_metadata.csv',
#        'tps6': 'metadata/Tps6_metadata.csv'
        'deepdom': 'LCdata/DeepDOMtraps_metadata.csv',
        'ventdom': 'LCdata/ventDOM_metadata.csv'
    }
    for e in exp_data.keys():
        data = exp_data[e]
        metadata = exp_metadata[e]
        def loggy(x):
            print x
        etl(session, e, data, metadata, log=loggy)
    # now commit
    session.commit()
    # now remove one of the experiments
    for e in exp_data.keys()[:1]:
        print 'Removing %s ...' % e
        remove_exp(session,e)
        session.commit()
        print 'Re-adding %s ...' % e
        data = exp_data[e]
        metadata = exp_metadata[e]
        def loggy(x):
            print x
        etl(session, e, data, metadata, log=loggy)
    # now commit
    session.commit()

DELETE=True

if __name__=='__main__':
    engine = get_sqlite_engine(delete=DELETE)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    if DELETE: # we deleted data, now need to load it
        print 'Loading data...'
        etl_examples(session)
    # now count how many entries we have
    n = session.query(func.count(Mtab.id)).first()[0]
    print '%d metabolites loaded' % n
    # now pick four metabolites at random
    while True:
        m = mtab_random(session)
        # for each one, find matching ones
        ms = list(match_one(session,m))
        if ms: # found matches. format as CSV
            out_recs = []
            # fixed schema
            out_schema = [
                'mtab_exp', # source mtab experiment name
                'mtab_mz', # source mtab m/z
                'mtab_rt', # source mtab retention time
                'match_exp', # matched mtab experiment name
                'match_mz', # matched mtab m/z
                'match_rt', # match mtab retention time
                'sample', # sample / datafile containing matched mtab
                'intensity' # intensity of matched mtab in that sample
            ]
            for match in ms:
                # now get metadata for matching metabolite
                for mi in session.query(MtabIntensity).\
                    filter(MtabIntensity.mtab_id==match.id):
                    # populate fixed schema
                    out_rec = {
                        'mtab_exp': m.exp.name,
                        'mtab_mz': m.mz,
                        'mtab_rt': m.rt,
                        'match_exp': match.exp.name,
                        'match_mz': match.mz,
                        'match_rt': match.rt,
                        'sample': mi.sample.name,
                        'intensity': mi.intensity
                    }
                    # now populate variable (per experiment) schema
                    for attr in mi.sample.attrs:
                        assert not attr.name in out_rec # fail fast if names collide
                        out_rec[attr.name] = attr.value
                        if attr.name not in out_schema: # keep track of all attributes we find
                            out_schema.append(attr.name)
                    out_recs.append(out_rec) # save record
            # now format the output records according to the accumulated union schema
            print ','.join(out_schema)
            for rec in out_recs:
                out_row = [rec.get(k,'') for k in out_schema]
                print ','.join(map(str,out_row)) # FIXME format numbers properly
            # for now exit because we're just doing one source mtab at a time in this test
            break
