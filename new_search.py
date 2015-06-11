from test import get_psql_engine
from jinja2 import Environment

from sql_templates import EXCLUDE_CONTROLS, INT_OVER_CONTROLS, ATTR_EXCLUDE_CONTROLS

def construct_search(mz,rt,ioc=None,ppm_diff=0.5,rt_range=30,attrs=None):
    if ioc is not None:
        query = Environment().from_string(INT_OVER_CONTROLS).render({
            'attrs': attrs
        })
        params = (mz,ppm_diff,rt,rt_range,ioc)
    elif attrs is not None and len(attrs) > 0:
        query = Environment().from_string(ATTR_EXCLUDE_CONTROLS).render({
            'attrs': attrs
        })
        params = (mz,ppm_diff,rt,rt_range)
    else:
        query = EXCLUDE_CONTROLS
        params = (mz,ppm_diff,rt,rt_range)
    return query, params

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

engine = get_psql_engine()
c = engine.connect()

for case in cases:
    query, params = construct_search(case.get('mz'),
                                     case.get('rt'),
                                     ioc=case.get('ioc'),
                                     attrs=case.get('attrs',[]))
    r = c.execute(query,*params)
    for row in r:
        print row

