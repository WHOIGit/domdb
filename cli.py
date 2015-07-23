import os
import sys
import cmd
import glob
import re

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from config import complete_config_key, set_config_key, initialize_config, save_config, get_default_config
from kuj_orm import Base, Exp, Mtab, DomDb, etl, initialize_schema, SampleAttr
from complete_path import complete_path
from utils import asciitable

import new_search

from engine import get_psql_engine

DEBUG=False
# ORM session management

def console_log(o):
    """so I can mix old and new-style print functions"""
    print str(o)

def get_engine():
    if DEBUG:
        engine = sqlalchemy.create_engine('sqlite://')
    else:
        #engine = get_sqlite_engine(delete=False)
        engine = get_psql_engine()
    return engine

def get_session_factory():
    engine = get_engine()
    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session

def list_exps(session,ion_mode):
    # list experiments, and stats about them
    def q():
        # for all experiments
        for exp in session.query(Exp).filter(Exp.ion_mode==ion_mode).all():
            n_samples = len(exp.samples) # count the samples
            # count the metabolites
            n_mtabs = session.query(func.count(Mtab.id)).filter(Mtab.exp==exp).first()[0]
            # return a row
            yield {
                'name': exp.name,
                'samples': n_samples,
                'metabolites': n_mtabs
            }
    # format the rows nicely
    for line in asciitable(list(q()),['name','samples','metabolites'],'Database is empty'):
        print line

def list_samples(session,exp_name,ion_mode):
    cols = ['name','control']
    rows = []
    exp = session.query(Exp).filter(Exp.name==exp_name).filter(Exp.ion_mode==ion_mode).first()
    if exp is None:
        print 'No such experiment %s' % exp_name
        return
    for sample in exp.samples:
        d = { 'name': sample.name,
              'control': sample.control }
        for a in sorted(sample.attrs,key=lambda a:a.name):
            if a.name not in cols:
                cols.append(a.name)
            d[a.name] = a.value
        rows.append(d)
    for line in asciitable(rows,cols,'No samples found'):
        print line

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
    def __init__(self,session_factory,ion_mode):
        cmd.Cmd.__init__(self)
        self.prompt = 'domdb> '
        self.session_factory = session_factory
        self.config = initialize_config()
        self.ion_mode = ion_mode
        self.do_count('')
    def do_count(self,args):
        with DomDb(self.session_factory, self.ion_mode, self.config) as domdb:
            if not args:
                n = domdb.mtab_count()
                print '%d metabolites in database' % n
            else:
                exp = args.split(' ')[0]
                n = domdb.mtab_count(exp)
                print '%d metabolites in experiment %s' % (n, exp)
    def do_list(self,args):
        session = self.session_factory()
        list_exps(session, self.ion_mode)
        session.close()
    def do_dir(self, args):
        dir = args
        result = list(list_exp_files(dir))
        print 'found files for %d experiments in %s' % (len(result), dir)
        for line in asciitable(result,disp_cols=['name','data','metadata']):
            print line
    def complete_dir(self, text, line, start_idx, end_idx):
        return complete_path(text, line)
    def do_add_dir(self, args):
        dir = args
        result = list(list_exp_files(dir))
        print 'found files for %d experiments in %s' % (len(result), dir)
        with DomDb(self.session_factory, self.ion_mode, self.config) as domdb:
            for d in result:
                name = d['name']
                path = os.path.join(dir,d['data'])
                mdpath = os.path.join(dir,d['metadata'])
                print 'loading experiment %s from:' % name
                print '- data file %s' % path
                print '- metadata file %s' % mdpath
                etl(domdb.session,name,path,mdpath,self.ion_mode,log=console_log)
                n = domdb.mtab_count(self.ion_mode)
                print '%d metabolites in database' % n
    def complete_add_dir(self, text, line, start_idx, end_idx):
        return complete_path(text, line)
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
        etl(session,exp,path,mdpath,self.ion_mode,log=console_log)
        session.close()
        with DomDb(self.session_factory, self.ion_mode, self.config) as domdb:
            n = domdb.mtab_count()
        print '%d metabolites in database' % n
    def complete_add(self, text, line, start_idx, end_idx):
        return complete_path(text, line)
    def complete_remove(self, text, line, start_idx, end_idx):
        session = self.session_factory()
        exps = session.query(Exp).filter(Exp.ion_mode==self.ion_mode).filter(Exp.name.like(text+'%')).all()
        return [e.name for e in exps]
    def do_remove(self,args):
        try:
            exp = args.split(' ')[0]
        except ValueError:
            print 'ERROR: remove takes [exp name]'
            return
        print 'Removing all %s data ...' % exp
        with DomDb(self.session_factory, self.ion_mode, self.config) as domdb:
            domdb.remove_exp(exp)
        self.do_list('')
    def _complete_attr(self, text):
        session = self.session_factory()
        return [r[0] for r in session.query(SampleAttr.name).\
                filter(SampleAttr.name.like(text+'%')).\
                order_by(SampleAttr.name).\
                distinct().all()]
    def complete_set(self, text, line, start_idx, end_idx):
        if re.match(r'.*attrs.*',line):
            return self._complete_attr(text)
        return complete_config_key(self.config, text)
    def _print_config(self):
        def massage(key,value):
            if key == 'attrs' and not value:
                return '(any)'
            try:
                return ','.join(value)
            except:
                return value
        ds = [dict(var=k,value=massage(k,v)) for k,v in sorted(self.config.items())]
        for line in asciitable(ds,disp_cols=['var','value']):
            print line
    def do_set(self,args):
        if not args:
            self._print_config()
        else:
            try:
                arglist = re.split(r' +',args)
                k = arglist[0]
                v = ' '.join(arglist[1:])
                set_config_key(self.config,k,v)
                save_config(self.config)
                self._print_config()
            except ValueError:
                print 'Syntax error: %s' % args
            except:
                raise
    def complete_reset(self, text, line, start_idx, end_idx):
        return complete_config_key(self.config, text)
    def do_reset(self,args):
        if args:
            self.config[args] = get_default_config()[args]
        else:
            self.config = get_default_config()
        save_config(self.config)
        self._print_config()
    def complete_samples(self, text, line, start_idx, end_idx):
        session = self.session_factory()
        return [r[0] for r in session.query(Exp.name).\
                filter(Exp.ion_mode==self.ion_mode).\
                filter(Exp.name.like(text+'%')).\
                order_by(Exp.name).\
                distinct().all()]
    def do_samples(self, args):
        exp_name = args
        if not exp_name:
            print 'Usage: samples [experiment name]'
            return
        session = self.session_factory()
        list_samples(session,exp_name,self.ion_mode)
        session.close()
    def do_exit(self,args):
        sys.exit(0)
    def do_quit(self,args):
        sys.exit(0)
    def do_search(self,args):
        try:
            arglist = re.split(r' +',args)
            mz = float(arglist[0])
            rt = int(arglist[1])
            outf = arglist[2]
        except IndexError:
            print 'usage: search [mz] [rt] [outfile]'
            return
        with open(outf,'w') as fout:
            r = new_search.search(get_engine(),mz,rt,self.ion_mode,self.config)
            for line in new_search.results_as_csv(r):
                print >>fout, line
    def do_match(self,args):
        try:
            arglist = re.split(r' +',args)
            exp_name = arglist[0]
            outf = arglist[1]
        except IndexError:
            print 'usage: match [exp_name] [outfile]'
            return
        with open(outf,'w') as fout:
            r = new_search.match(get_engine(),exp_name,self.ion_mode,self.config)
            for line in new_search.results_as_csv(r):
                print >>fout, line

def get_ion_mode(s):
    if s in ['neg','pos']:
        return s
    else:
        raise ValueError('ion mode must be "neg" or "pos"')

if __name__=='__main__':
    engine = get_engine()
    initialize_schema(engine)
    try:
        ion_mode = get_ion_mode(sys.argv[1])
    except IndexError:
        print 'Usage: python cli.py [ion mode: either "neg" or "pos"]'
        sys.exit(-1)
    shell = Shell(get_session_factory(),ion_mode)
    shell.cmdloop('DOMDB v1')
