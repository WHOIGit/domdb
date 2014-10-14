import os
import sys
import cmd

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from test import get_sqlite_engine
from kuj_orm import Base, Mtab, MtabIntensity, Exp, etl, mtab_search, mtab_random, match_all_from, match_one

DEBUG=False

def get_session_factory():
    if DEBUG:
        engine = sqlalchemy.create_engine('sqlite://')
    else:
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
        q = q.filter(Mtab.exp.has(name=exp))
    return q.first()[0]

def list_exps(session):
    for exp_id, count in session.query(Mtab.exp_id, func.count()).\
        group_by(Mtab.exp_id):
        exp = session.query(Exp).filter(Exp.id==exp_id).first()
        print '\t'.join((exp.name,str(count)))

def matches_as_csv(session,pairs):
    for m, match in pairs:
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
        # now we have all the output records in hand
        # format the output records according to the accumulated union schema
        yield ','.join(out_schema)
        for rec in out_recs:
            out_row = [rec.get(k,'') for k in out_schema]
            yield ','.join(map(str,out_row)) # FIXME format numbers better
    
def console_log(message):
    print message

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
        exp, path, mdpath = args.split(' ')
        if not os.path.exists(path):
            print 'data file %s does not exist' % path
        if not os.path.exists(mdpath):
            print 'metadata file %s does not exist' % mdpath
        else:
            print 'loading experiment %s from:' % exp
            print 'data file %s' % path
            print 'metadata file %s' % mdpath
            session = self.session_factory()
            etl(session,exp,path,mdpath,log=console_log)
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
        exp, outf = args.split(' ')
        session = self.session_factory()
        print 'Searching for matches from %s, please wait ...' % exp
        matches = list(match_all_from(session,exp))
        if not matches:
            print 'No matches found'
        else:
            print 'Found %d matches' % len(matches)
            with open(outf,'w') as fout:
                print 'Saving results to %s ...' % outf
                for line in matches_as_csv(session,matches):
                    print >> fout, line
        session.close()
    def do_remove(self,args):
        exp = args.split(' ')[0]
        print 'Removing all %s data ...' % exp
        session = self.session_factory()
        session.query(Mtab).filter(Mtab.exp.has(name=exp)).delete(synchronize_session='fetch')
        session.query(Exp).filter(Exp.name==exp).delete(synchronize_session='fetch')
        session.commit()
        self.do_list('')
        session.close()
    def do_test(self,args):
        session = self.session_factory()
        print 'Randomly matching metabolites...'
        while True:
            mtab = mtab_random(session)
            ms = list(match_one(session,mtab))
            if ms:
                print '%s matched the following:' % mtab
                for m in ms:
                    print '* %s' % m
                break
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
