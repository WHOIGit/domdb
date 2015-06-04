from jinja2 import Environment

# positional SQL params
# 1. m/z ratio
# 2. m/z ppm range
# 3. retention time
# 4. rt range
# 5. intensity over controls
# template params
# attrs: names of sample attrs to group by
template = """
-- first subquery: find matching mz/rt values, then collate with specific sample attributes
with sq1 as (
select exp_id, mtab_id, mz, rt, withms2, a.sample_id, intensity, control{% for attr in attrs %},
  (select value from sample_attr b where a.sample_id=b.sample_id and name='{{attr}}') as attr_{{attr}}{% endfor %}
from mtab_sample_intensity a
where

-- mz/rt clause
abs(1e6 * (%s - a.mz) / a.mz) <= %s
and abs(a.rt - %s) <= %s
),
-- second subquery: compute average intensity in each sample attribute grouping
sq2 as (
select mtab_id, control{% for attr in attrs %}, attr_{{attr}}{% endfor %}, avg(intensity) as avg_intensity
from sq1
group by mtab_id, control{% for attr in attrs %}, attr_{{attr}}{% endfor %}
),
-- third subquery: collate first subquery with second subquery, selecting average intensity where control=1
-- to generate "intensity in controls" column (iic) that is grouped by sample attribute
sq3 as (
select exp_id, sq1.mtab_id, mz, rt, withms2, sample_id, intensity, sq1.control{% for attr in attrs %}, sq1.attr_{{attr}}{% endfor %},
  (select avg_intensity from sq2
   where sq1.mtab_id=sq2.mtab_id{% for attr in attrs %}
   and sq1.attr_{{attr}}=sq2.attr_{{attr}}{% endfor %}
   and sq2.control=1) as iic
from sq1
)
-- query: filter third subquery by intensity over controls
select exp_id, mtab_id, mz, rt, withms2, sample_id{% for attr in attrs %}, attr_{{attr}}{% endfor %}, intensity, control
from sq3
where intensity > %s * iic
order by exp_id, mtab_id{% for attr in attrs %}, attr_{{attr}}{% endfor %}, sample_id
"""

ppm_diff = 0.5
rt_range = 30

mz = 116
rt = 100

ioc = 10

attrs = ['media','time']

query = Environment().from_string(template).render({
    'attrs': attrs
})

params = (mz,ppm_diff,rt,rt_range,ioc)

print query % tuple(map(str,params))
