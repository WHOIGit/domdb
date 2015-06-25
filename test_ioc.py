from test import get_psql_engine
from jinja2 import Environment

from sql_templates import INT_OVER_CONTROLS

def construct_search(mz,rt,ioc=None,attrs=None,ppm_diff=0.5,rt_diff=30):
    query = Environment().from_string(INT_OVER_CONTROLS).render({
        'attrs': attrs
    })
    params = (mz,ppm_diff,rt,rt_diff,ioc)
    return query, params

def search(engine,mz,rt,ioc=None,ppm_diff=0.5,rt_diff=30,attrs=None):
    """returns ResultProxy"""
    c = engine.connect()
    query, params = construct_search(mz,rt,ioc=ioc,ppm_diff=ppm_diff,rt_diff=rt_diff,attrs=attrs)
    return c.execute(query,*params)

def test(engine):
    cases = [
        dict(mz=116,rt=100,attrs=['media','time'],ioc=10),
        dict(mz=105,rt=80,ioc=10),
        dict(mz=113,rt=100,attrs=['media'],ioc=10),
        dict(mz=113,rt=200,attrs=['media'],ioc=10),
        dict(mz=114,rt=100,attrs=['media'],ioc=10),
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

if __name__=='__main__':
    engine = get_psql_engine()
    test(engine)
