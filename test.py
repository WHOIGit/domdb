import os

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from kuj_orm import Base, Mtab, MtabIntensity
from kuj_orm import etl, match_one

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

DELETE=False

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
        m = session.query(Mtab).order_by(func.random()).limit(1).first()
        # for each one, find matching ones
        ms = list(match_one(session,m))
        if ms:
            print 'Found matches for %s:' % m
            for match in ms:
                # now get metadata for matching metabolites
                for mi in session.query(MtabIntensity).\
                    filter(MtabIntensity.mtab_id==m.id):
                    print '%s matched %s {' % (m,match)
                    print 'sample: %s' % (mi.sample.name)
                    print 'intensity: %s' % (mi.intensity)
                    for attr in mi.sample.attrs:
                        print '"%s": "%s"' % (attr.name, attr.value)
                    print '}'
            break
