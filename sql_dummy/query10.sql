--query 10
with sq1 as (
select exp_id, mtab_id, mz, rt, withms2, a.sample_id, intensity, control,
  (select value from sample_attr b where a.sample_id=b.sample_id and name='media') as attr_media,
  (select value from sample_attr b where a.sample_id=b.sample_id and name='time') as attr_time
from mtab_sample_intensity a
where

-- mz/rt clause
abs(a.rt - 500) <= 30
and abs(1e6 * (109 - a.mz) / a.mz) <= 0.5

-- exclude zero-intensity samples
and intensity > 0

--
order by exp_id, mtab_id, sample_id, control, attr_media, attr_time
), sq2 as (
select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control, attr_media, attr_time,
avg(intensity * (control=1)::int) over (partition by attr_media, attr_time) as iic
from sq1
)
select * from sq2 where iic=0
