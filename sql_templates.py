# positional SQL params
# 1. m/z ratio
# 2. m/z ppm range
# 3. retention time
# 4. rt range
# 5. intensity over controls (for some queries)
# template params
# attrs: names of sample attrs to group by (for some queries)

# mtab_exp,mtab_mz,mtab_rt,mtab_annotated,match_exp,match_mz,match_rt,match_annotated,sample,intensity,
EXCLUDE_CONTROLS="""
select (select name from experiment e where e.id=exp_id) as match_exp,
       mz as match_mz, rt as match_rt, annotated as match_annotated, withms2 as match_withms2,
       (select name from sample s where s.id=sample_id) as sample,
       intensity, control,
       (select attrs from agg_sample_attr asa where asa.sample_id=a.sample_id)
from mtab_sample_intensity a
where
  -- mz/rt clause
  1e6 * abs(%s - a.mz) <= a.mz * %s
  and abs(a.rt - %s) <= %s
  -- exclude mtabs that occur in controls
  and (select max((control=1)::int) from mtab_sample_intensity b
       where a.mtab_id=b.mtab_id
       and intensity > 0) = 0
  -- exclude zero-intensity samples
  and intensity > 0
order by exp_id, sample_id"""

ATTR_EXCLUDE_CONTROLS="""with sq1 as (
select exp_id, mtab_id, mz, rt, annotated, withms2, a.sample_id, intensity, control{% for attr in attrs %},
  (select value from sample_attr b where a.sample_id=b.sample_id and name='{{attr}}') as attr_{{attr}}{% endfor %}
from mtab_sample_intensity a
where
  -- mz/rt clause
  1e6 * abs(%s - a.mz) <= a.mz * %s
  and abs(a.rt - %s) <= %s

  and intensity > 0
), sq2 as (
select exp_id, mtab_id, mz, rt, annotated, withms2, sample_id, intensity, control{% for attr in attrs %}, attr_{{attr}}{% endfor %},
       avg(intensity * (control=1)::int) over (partition by {% for attr in attrs[:1] %}attr_{{attr}}{% endfor %}{% for attr in attrs[1:] %}, attr_{{attr}}{% endfor %}) as iic
from sq1
)
select (select name from experiment e where e.id=exp_id) as match_exp,
       mz as match_mz, rt as match_rt, annotated as match_annotated, withms2 as match_withms2,
       (select name from sample s where s.id=sample_id) as sample,
       intensity, control,
       (select attrs from agg_sample_attr asa where asa.sample_id=sq2.sample_id)
from sq2
where iic=0
order by exp_id, sample_id
"""

INT_OVER_CONTROLS="""
with
q1 as (select mtab_id, i.sample_id, intensity, control{% for a in attrs %},
             (select value from sample_attr sa where sa.sample_id=i.sample_id and sa.name='{{a}}') as attr_{{a}}{% endfor %}
       from intensity i, sample s
       where i.sample_id = s.id),

q2 as (select mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}, avg(intensity) as iic
       from q1
       where control=1
       group by mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}),

q3 as (select mtab_id, sample_id
       from q1
       where mtab_id in (select id from metabolite m
                         where 1e6 * abs(m.mz - %s) <= %s * m.mz
                         and abs(m.rt - %s) <= %s)
       and control=0
       and intensity > %s * (select iic from q2 where q1.mtab_id=q2.mtab_id{% for a in attrs %}
                             and q1.attr_{{a}} is not distinct from q2.attr_{{a}}{% endfor %})
       order by mtab_id, sample_id)

-- friendly output
select match_exp, match_mz, match_rt, match_annotated, "match_withMS2", sample, intensity, control, attrs
from mtab_sample_attr msa, q3
where msa.mtab_id=q3.mtab_id
and msa.sample_id=q3.sample_id
"""

CREATE_VIEWS=["""
create or replace view mtab_sample_attr
as
select m.id as mtab_id, s.id as sample_id,
       e.name as match_exp,
       m.mz as match_mz, m.rt as match_rt, m.annotated as match_annotated, m."withMS2" as "match_withMS2",
       s.name as sample,
       intensity,
       control,
       (select array_agg(sa.name || '=' || sa.value) from sample_attr sa where sa.sample_id=s.id) as attrs
from experiment e, sample s, metabolite m, intensity i
where s.exp_id=e.id
and i.sample_id=s.id
and i.mtab_id=m.id
"""]
