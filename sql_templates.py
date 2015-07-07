# positional SQL params
# 1. m/z ratio
# 2. m/z ppm range
# 3. retention time
# 4. rt range
# 5. intensity over controls (for some queries)
# template params
# attrs: names of sample attrs to group by (for some queries)

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
