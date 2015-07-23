from engine import get_psql_engine
from jinja2 import Environment

from sql_templates import SIMPLE_SEARCH_TEMPLATE, SEARCH_TEMPLATE, SIMPLE_MATCH_TEMPLATE, MATCH_TEMPLATE

from config import PPM_DIFF,RT_DIFF,WITH_MS2,EXCLUDE_CONTROLS,INT_OVER_CONTROLS,ATTRS

def construct_search(mz,rt,ion_mode,config):
    ppm_diff = config.get(PPM_DIFF)
    rt_diff = config.get(RT_DIFF)
    ioc = config.get(INT_OVER_CONTROLS)
    attrs = config.get(ATTRS)
    with_ms2 = config.get(WITH_MS2)
    exclude_controls = config.get(EXCLUDE_CONTROLS)
    if not exclude_controls:
        query = Environment().from_string(SIMPLE_SEARCH_TEMPLATE).render({
            'with_ms2': with_ms2
        })
        params = (ion_mode,mz,ppm_diff,rt,rt_diff)
    else:
        query = Environment().from_string(SEARCH_TEMPLATE).render({
            'attrs': attrs,
            'ioc': ioc,
            'with_ms2': with_ms2
        })
        if ioc is not None:
            params = (ion_mode,mz,ppm_diff,rt,rt_diff,ioc)
        else:
            params = (ion_mode,mz,ppm_diff,rt,rt_diff)
    return query, params

def construct_match(exp_name,ion_mode,config):
    ppm_diff = config.get(PPM_DIFF)
    rt_diff = config.get(RT_DIFF)
    ioc = config.get(INT_OVER_CONTROLS)
    attrs = config.get(ATTRS)
    with_ms2 = config.get(WITH_MS2)
    exclude_controls = config.get(EXCLUDE_CONTROLS)
    if not exclude_controls:
        query = Environment().from_string(SIMPLE_MATCH_TEMPLATE).render({
            'with_ms2': with_ms2
        })
        params = (ion_mode,exp_name,ion_mode,ppm_diff,rt_diff)
    else:
        query = Environment().from_string(MATCH_TEMPLATE).render({
            'attrs': attrs,
            'ioc': ioc,
            'with_ms2': with_ms2
        })
        if ioc is not None:
            params = (ion_mode,exp_name,ioc,ion_mode,ppm_diff,rt_diff)
        else:
            params = (ion_mode,exp_name,ion_mode,ppm_diff,rt_diff)
    return query, params

def search(engine,mz,rt,ion_mode,config):
    """returns ResultProxy"""
    c = engine.connect()
    query, params = construct_search(mz,rt,ion_mode,config)
    return c.execute(query,*params)

def match(engine,exp_name,ion_mode,config):
    c = engine.connect()
    query, params = construct_match(exp_name,ion_mode,config)
    return c.execute(query,*params)

def results_as_csv(r):
    rows = r.fetchall()
    cols = [x for x in r.keys() if x != 'attrs']
    if not rows:
        yield ','.join(cols)
        return
    # determine column headings
    attrs = []
    for row in rows:
        for kv in dict(row.items()).get('attrs'):
            k, v = kv.split('=')
            if k not in attrs:
                attrs += [k]
    cols += attrs
    yield ','.join(cols)
    # now postprocess rows
    for row in rows:
        rd = dict(row.items())
        # explode attrs
        ad = dict([kv.split('=') for kv in rd['attrs']])
        del rd['attrs']
        rd.update(ad) # FIXME avoid name collisions
        yield ','.join(str(rd.get(c,'')) for c in cols)

def test(engine):
    cases = [
        dict(mz=100,rt=90),
        dict(mz=102,rt=90),
        dict(mz=102,rt=200),
        dict(mz=103,rt=100),
        dict(mz=105,rt=80,ioc=10),
        dict(mz=107,rt=200,attrs=['media']),
        dict(mz=108,rt=100,attrs=['media']),
        dict(mz=109,rt=500,attrs=['media','time']),
        dict(mz=111,rt=500,attrs=['media','time']),
        dict(mz=113,rt=100,attrs=['media'],ioc=10),
        dict(mz=113,rt=200,attrs=['media'],ioc=10),
        dict(mz=114,rt=100,attrs=['media'],ioc=10),
        dict(mz=116,rt=100,attrs=['media','time'],ioc=10)
    ]
    for case in cases:
        print case
        r = search(engine,
                   case.get('mz'),
                   case.get('rt'),
                   ioc=case.get('ioc'),
                   attrs=case.get('attrs',[]))
        for line in results_as_csv(r):
            print line

if __name__=='__main__':
    engine = get_psql_engine()
    test(engine)

