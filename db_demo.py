import os
import sys
import readline
import cmd

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from kuj_orm import get_sqlite_engine, Base, Mtab, etl, mtab_search, mtab_random, match_all_from

def get_session_factory():
    engine = get_sqlite_engine(delete=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session

# lifting path completion from
# https://stackoverflow.com/questions/16826172/filename-tab-completion-in-cmd-cmd-of-python
def _complete_path(text, line):
    arg = line.split()[1:]
    dir, base = '', ''
    try: 
        dir, base = os.path.split(arg[-1])
    except:
        pass
    cwd = os.getcwd()
    try: 
        os.chdir(dir)
    except:
        pass
    ret = [f+os.sep if os.path.isdir(f) else f for f in os.listdir('.') if f.startswith(base)]
    if base == '' or base == '.': 
        ret.extend(['./', '../'])
    elif base == '..':
        ret.append('../')
    os.chdir(cwd)
    return ret

def mtab_count(session,exp=None):
    q = session.query(func.count(Mtab.id))
    if exp is not None:
        q = q.filter(Mtab.experiment == exp)
    return q.first()[0]

def list_exps(session):
    for exp, count in session.query(Mtab.experiment, func.count()).\
        group_by(Mtab.experiment):
        print '\t'.join((exp,str(count)))

class Shell(cmd.Cmd):
    def __init__(self,session_factory):
        cmd.Cmd.__init__(self)
        self.session_factory = session_factory
        self.do_count('')
    def do_list(self,args):
        session = self.session_factory()
        list_exps(session)
        session.close()
    def do_count(self,args):
        session = self.session_factory()
        if not args:
            n = mtab_count(session)
            print '%d metabolites in database' % n
        else:
            exp = args.split(' ')[0]
            n = mtab_count(session, exp)
            print '%d metabolites in experiment %s' % (n, exp)
        session.close()
    def do_add(self,args):
        exp, path = args.split(' ')
        if not os.path.exists(path):
            print 'file %s does not exist' % path
        else:
            print 'adding data in %s as experiment %s' % (path, exp)
            session = self.session_factory()
            etl(session,exp,path)
            n = session.query(func.count(Mtab.id)).first()[0]
            print '%d metabolites in database' % n
            session.close()
    def complete_add(self, text, line, start_idx, end_idx):
        return _complete_path(text, line)
    def do_search(self,args):
        mz, rt = args.split(' ')
        mz = float(mz)
        rt = float(rt)
        session = self.session_factory()
        for m in mtab_search(session,mz,rt):
            print m
        session.close()
    def do_all(self,args):
        exp = args.split(' ')[0]
        session = self.session_factory()
        print 'Searching for matches from %s' % exp
        for m in match_all_from(session,exp):
            print m
        session.close()
    def do_remove(self,args):
        exp = args.split(' ')[0]
        print 'Removing all %s data ...' % exp
        session = self.session_factory()
        session.query(Mtab).filter(Mtab.experiment==exp).delete()
        session.commit()
        self.do_list('')
        session.close()
    def do_random(self,args):
        session = self.session_factory()
        print mtab_random(session)
        session.close()
    def do_exit(self,args):
        sys.exit(0)
    def do_quit(self,args):
        sys.exit(0)

if __name__=='__main__':
    shell = Shell(get_session_factory())
    shell.cmdloop('Hi Krista')
