import os
import sys
import cmd
import glob
import re

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from test import get_sqlite_engine
from kuj_orm import Base, Mtab, MtabIntensity, Exp
#from kuj_orm import etl, Db, mtab_search, mtab_random, match_all_from, match_one, remove_exp, mtab_dist
from kuj_orm import etl, DomDb
from kuj_orm import PPM_DIFF, RT_DIFF, WITH_MS2, default_config

from utils import asciitable

DEBUG=False

# ORM session management
def get_session_factory():
    if DEBUG:
        engine = sqlalchemy.create_engine('sqlite://')
    else:
        engine = get_sqlite_engine(delete=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session

# tab-completion in command line

# Mac OS readline support requires serious hacking, cribbed from
# http://stackoverflow.com/questions/7116038/python-tab-completion-mac-osx-10-7-lion
import readline
import rlcompleter
if not readline.__doc__: # Windows
	COMPLETE_ABS=False
elif readline.__doc__ and 'libedit' in readline.__doc__: # MacOS
    readline.parse_and_bind("bind ^I rl_complete")
    COMPLETE_ABS=True
else: # *nix
    readline.parse_and_bind("tab: complete")
    COMPLETE_ABS=False

# lifting path completion from
# https://stackoverflow.com/questions/16826172/filename-tab-completion-in-cmd-cmd-of-python
def _complete_path(text, line):
    arg = line.split()[1:]
    if not arg:
        completions = os.listdir('.'+os.sep)
    else:
        dir, part, base = arg[-1].rpartition(os.sep)
        if part == '':
            dir = '.'+os.sep
        elif dir == '':
            dir = os.sep          
        completions = []
        for f in os.listdir(dir):
            if f.startswith(base):
                cpath = os.path.join(dir,f)
                if COMPLETE_ABS:
                    addpath = cpath
                else:
                    addpath = f
                if os.path.isfile(cpath):
                    completions.append(addpath)
                else:
                    completions.append(addpath+os.sep)
    return completions

# ORM utilities

def list_exps(session):
    def q():
        for exp in session.query(Exp).all():
            n_samples = len(exp.samples)
            n_mtabs = session.query(func.count(Mtab.id)).filter(Mtab.exp==exp).first()[0]
            yield {
                'name': exp.name,
                'samples': n_samples,
                'metabolites': n_mtabs
            }
    for line in asciitable(list(q()),['name','samples','metabolites'],'Database is empty'):
        print line

def avoid_name_collisions(name,schema):
    n = 1
    newname = name
    while newname in schema:
        newname = '%s_%d' % (name, n)
        n += 1
    return newname

def matches_as_csv(pairs):
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
    for m, match in pairs:
        # get metadata for matching metabolite
        for mi in match.intensities:
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
    yield ','.join(out_schema)
    for rec in out_recs:
        out_row = [rec.get(k,'') for k in out_schema]
        yield ','.join(map(str,out_row)) # FIXME format numbers better

def search_out_csv(matches,outf=None):
    if not matches:
        print 'No matches found'
        return
    print 'Found %d matches' % len(matches)
    outlines = matches_as_csv(matches)
    if outf is not None:
        with open(outf,'w') as fout:
            print 'Saving results to %s ...' % outf
            for line in outlines:
                print >> fout, line
    else:
        for line in outlines:
            print line

# command-line interface

def console_log(message):
    print message

CONFIG_TYPES={
    PPM_DIFF: float,
    RT_DIFF: float,
    WITH_MS2: bool
}

def list_exp_files(dir):
    """lists all experiments. assumes filenames are in the format
    {exp_name}_{anything}.csv = data file
    {exp_name}_{anything including "metadata"}.csv = metadata file
    converts exp name to lowercase.
    returns basenames of files (without directory)"""
    result = {}
    for fn in glob.glob(os.path.join(dir,'*.csv')):
        bn = os.path.basename(fn)
        bnl = bn.lower()
        name = re.sub('_.*','',bnl)
        if name not in result:
            result[name] = {}
        if bnl.find('metadata') >= 0:
            result[name]['metadata'] = fn
        else:
            result[name]['data'] = fn 
    for exp,v in result.items():
        if 'data' in v and 'metadata' in v:
            yield {
                'name': exp,
                'data': os.path.basename(v['data']),
                'metadata': os.path.basename(v['metadata'])
                }

class Shell(cmd.Cmd):
    def __init__(self,session_factory):
        cmd.Cmd.__init__(self)
        self.session_factory = session_factory
        self.config = default_config()
        self.do_count('')
    def do_list(self,args):
        session = self.session_factory()
        list_exps(session)
        session.close()
    def do_config(self,args):
        try:
            key, value = args.split(' ')
            if key not in self.config:
                print 'ERROR: unknown parameter %s' % key
            else:
                try:
                    if CONFIG_TYPES[key]==bool:
                        self.config[key] = value in ['True','true','T','t','1','Yes','yes','Y','y']
                    else:
                        self.config[key] = CONFIG_TYPES[key](value)
                    print 'set %s to %s' % (key, self.config[key])
                except ValueError:
                    print 'ERROR: bad value for %s: "%s"' % (key,value)
                    return
        except ValueError:
            pass
        rows = [dict(param=k,value=v) for k,v in self.config.items()]
        for line in asciitable(rows,['param','value']):
            print line
    def do_count(self,args):
        with DomDb(self.session_factory, self.config) as domdb:
            if not args:
                n = domdb.mtab_count()
                print '%d metabolites in database' % n
            else:
                exp = args.split(' ')[0]
                n = domdb.mtab_count(exp)
                print '%d metabolites in experiment %s' % (n, exp)
    def do_dir(self, args):
        dir = args
        result = list(list_exp_files(dir))
        print 'found files for %d experiments in %s' % (len(result), dir)
        for line in asciitable(result,disp_cols=['name','data','metadata']):
            print line
    def complete_dir(self, text, line, start_idx, end_idx):
        return _complete_path(text, line)
    def do_add_dir(self, args):
        dir = args
        result = list(list_exp_files(dir))
        print 'found files for %d experiments in %s' % (len(result), dir)
        with DomDb(self.session_factory, self.config) as domdb:
            for d in result:
                name = d['name']
                path = os.path.join(dir,d['data'])
                mdpath = os.path.join(dir,d['metadata'])
                print 'loading experiment %s from:' % name
                print '- data file %s' % path
                print '- metadata file %s' % mdpath
                etl(domdb.session,name,path,mdpath,log=console_log)
                n = domdb.session.query(func.count(Mtab.id)).first()[0]
                print '%d metabolites in database' % n
    def complete_add_dir(self, text, line, start_idx, end_idx):
        return _complete_path(text, line)
    def do_add(self,args):
        try:
            exp, path, mdpath = args.split(' ')
        except ValueError:
            print 'ERROR: add takes [exp name] [data file] [metadata file]'
            return
        if not os.path.exists(path):
            print 'data file %s does not exist' % path
            return
        if not os.path.exists(mdpath):
            print 'metadata file %s does not exist' % mdpath
            return
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
        outf = None
        try:
            mz, rt = args.split(' ')
        except ValueError:
            try:
                mz, rt, outf = args.split(' ')
            except ValueError:
                print 'ERROR: search takes [mz] [rt] [outfile (optional)]'
                return
        mz = float(mz)
        rt = float(rt)
        fake_exp = Exp(name='N/A')
        fake_mtab = Mtab(rt=rt, mz=mz, exp=fake_exp, annotated='')
        with DomDb(self.session_factory, self.config) as domdb:
            q = domdb.mtab_search(mz,rt)
            pairs = [(fake_mtab, m) for m in q]
            search_out_csv(pairs,outf)
    def do_all(self,args):
        try:
            exp, outf = args.split(' ')
        except ValueError:
            print 'ERROR: all takes [exp name] [outfile]'
            return
        print 'Searching for matches from %s, please wait ...' % exp
        with DomDb(self.session_factory, self.config) as domdb:
            q = domdb.match_all_from(exp)
            search_out_csv(list(q),outf)
    def do_remove(self,args):
        try:
            exp = args.split(' ')[0]
        except ValueError:
            print 'ERROR: remove takes [exp name]'
            return
        print 'Removing all %s data ...' % exp
        with DomDb(self.session_factory, self.config) as domdb:
            domdb.remove_exp()
            remove_exp(session,exp)
        self.do_list('')
    def do_test(self,args):
        with DomDb(self.session_factory, self.config) as domdb:
            print 'Randomly matching metabolites...'
            while True:
                mtab = domdb.mtab_random()
                ms = list(domdb.match_one(mtab))
                if ms:
                    print '%s matched the following:' % mtab
                    for m in ms:
                        print '* %s' % m
                    break
    def do_random(self,args):
        with DomDb(self.session_factory, self.config) as domdb:
            print domdb.mtab_random()
    def do_pdf(self,args):
        with DomDb(self.session_factory, self.config) as domdb:
            pdf = domdb.mtab_dist(4000)
            for k in sorted(pdf.keys()):
                print '%d: %d' % (k, pdf[k])
    def do_exit(self,args):
        sys.exit(0)
    def do_quit(self,args):
        sys.exit(0)

if __name__=='__main__':
    shell = Shell(get_session_factory())
    shell.cmdloop('Hi Krista')
