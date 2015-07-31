import os

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db import DATABASE_URL

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

def get_psql_engine():
    return sqlalchemy.create_engine(DATABASE_URL)
