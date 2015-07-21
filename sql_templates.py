# positional SQL params
# 1. m/z ratio
# 2. m/z ppm range
# 3. retention time
# 4. rt range
# 5. intensity over controls (for some queries)
# template params
# attrs: names of sample attrs to group by (for some queries)
# ioc: None if not using ioc but just excluding controls, some Truey value otherwise
# FIXME: with_ms2: T or F whether to require with_ms2 to be true

SEARCH_TEMPLATE="""
with
q0 as (select id from metabolite m
       where 1e6 * abs(m.mz - %s) <= %s * m.mz
       and abs(m.rt - %s) <= %s),

q1 as (select mtab_id, i.sample_id, intensity, control{% for a in attrs %},
             (select value from sample_attr sa where sa.sample_id=i.sample_id and sa.name='{{a}}') as attr_{{a}}{% endfor %}
       from intensity i, sample s, q0
       where mtab_id=q0.id and i.sample_id = s.id),

q2 as (select mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}, avg(intensity) as iic
       from q1
       where control=1
       group by mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}),

q3 as (select mtab_id, sample_id
       from q1
       where control=0
{% if ioc %}
       and intensity > %s * (select iic from q2 where q1.mtab_id=q2.mtab_id{% for a in attrs %}
                             and q1.attr_{{a}} is not distinct from q2.attr_{{a}}{% endfor %})
{% else %}
       and intensity > 0
       and 0 = (select iic from q2 where q1.mtab_id=q2.mtab_id{% for a in attrs %}
                and q1.attr_{{a}} is not distinct from q2.attr_{{a}}{% endfor %})
{% endif %}
       order by mtab_id, sample_id)

-- friendly output
select match_exp, match_mz, match_rt, match_annotated, "match_withMS2", sample, intensity, control, attrs
from mtab_sample_attr msa, q3
where msa.mtab_id=q3.mtab_id
and msa.sample_id=q3.sample_id
"""

# positional SQL params
# 1. name of experiment to match from
# 2. (optional) ioc
# 3. m/z ppm range
# 4. rt range
# template params
# attrs: names of sample attrs to group by (for some queries)
# ioc: None if not using ioc but just excluding controls, some Truey value otherwise

MATCH_TEMPLATE="""
with
q1 as (select mtab_id, i.sample_id, intensity, control{% for a in attrs %},
             (select value from sample_attr sa where sa.sample_id=i.sample_id and sa.name='{{a}}') as attr_{{a}}{% endfor %}
       from intensity i, sample s
       where s.exp_id=(select id from experiment where name=%s)
       and i.sample_id = s.id),

q2 as (select mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}, avg(intensity) as iic
       from q1
       where control=1
       group by mtab_id{% for a in attrs %}, attr_{{a}}{% endfor %}),

q3 as (select mtab_id
       from q1
       where control=0
{% if ioc %}
       and intensity > %s * (select iic from q2 where q1.mtab_id=q2.mtab_id{% for a in attrs %}
                             and q1.attr_{{a}} is not distinct from q2.attr_{{a}}{% endfor %})
{% else %}
       and intensity > 0
       and 0 = (select iic from q2 where q1.mtab_id=q2.mtab_id{% for a in attrs %}
                and q1.attr_{{a}} is not distinct from q2.attr_{{a}}{% endfor %})
{% endif %}
       group by mtab_id
       having count(*) > 0),

q4 as (select a.id, b.id as match_id
       from metabolite a, metabolite b
       where a.id in (select mtab_id from q3)
       and a.id <> b.id
       and b.id not in (select mtab_id from q3)
       and 1e6 * abs(a.mz - b.mz) <= %s * a.mz
       and abs(a.rt - b.rt) <= %s)

-- friendly output
select a.id, a.mz, a.rt,
       match_exp, match_mz, match_rt, match_annotated, "match_withMS2", sample, intensity, control, attrs
from q4, metabolite a, mtab_sample_attr b
where q4.id=a.id
and q4.match_id=b.mtab_id
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
