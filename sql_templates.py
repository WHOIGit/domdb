# positional SQL params
# 1. m/z ratio
# 2. m/z ppm range
# 3. retention time
# 4. rt range
# 5. intensity over controls (for some queries)
# template params
# attrs: names of sample attrs to group by (for some queries)

EXCLUDE_CONTROLS="""select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control,
         (select attrs from agg_sample_attr asa where asa.sample_id=a.sample_id)
from mtab_sample_intensity a
where
  -- mz/rt clause
  abs(1e6 * (%s - a.mz) / a.mz) <= %s
  and abs(a.rt - %s) <= %s
  -- exclude mtabs that occur in controls
  and (select max((control=1)::int) from mtab_sample_intensity b
       where a.mtab_id=b.mtab_id
       and intensity > 0) = 0
  -- exclude zero-intensity samples
  and intensity > 0
order by exp_id, mtab_id, sample_id"""

INT_OVER_CONTROLS="""
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
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control,
       (select attrs from agg_sample_attr asa where asa.sample_id=sq3.sample_id)
from sq3
where intensity > %s * iic
order by exp_id, mtab_id, sample_id
"""

ATTR_EXCLUDE_CONTROLS="""with sq1 as (
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
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control{% for attr in attrs %}, attr_{{attr}}{% endfor %},
       avg(intensity * (control=1)::int) over (partition by {% for attr in attrs[:1] %}attr_{{attr}}{% endfor %}{% for attr in attrs[1:] %}, attr_{{attr}}{% endfor %}) as iic
from sq1
)
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control,
      (select attrs from agg_sample_attr asa where asa.sample_id=sq2.sample_id)
from sq2
where iic=0
order by exp_id, mtab_id, sample_id
"""
