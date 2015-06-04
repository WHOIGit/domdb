from jinja2 import Environment

# template params
# attrs list of sample attributes to group by
template="""with sq1 as (
select exp_id, mtab_id, mz, rt, withms2, a.sample_id, intensity, control{% for attr in attrs %},
  (select value from sample_attr b where a.sample_id=b.sample_id and name='{{attr}}') as attr_{{attr}}{% endfor %}
from mtab_sample_intensity a
where

-- mz/rt clause
abs(1e6 * (%s - a.mz) / a.mz) <= %s
and abs(a.rt - %s) <= %s

-- exclude zero-intensity samples
and intensity > 0
), sq2 as (
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control{% for attr in attrs %}, attr_{{attr}}{% endfor %}, avg(intensity * (control=1)::int) over (partition by {% for attr in attrs[:1] %}attr_{{attr}}{% endfor %}{% for attr in attrs[1:] %}, attr_{{attr}}{% endfor %}) as iic
from sq1
)
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control
from sq2
where iic=0
order by exp_id, mtab_id, sample_id
"""

ppm_diff = 0.5
rt_range = 30

mz = 107
rt = 200

attrs = ['media']

query = Environment().from_string(template).render({
    'attrs': attrs
})

params = (mz,ppm_diff,rt,rt_range)

print query % tuple(map(str,params))

