--query 15

-- first subquery: find matching mz/rt values, then collate with specific sample attributes
with sq1 as (
select exp_id, mtab_id, mz, rt, withms2, a.sample_id, intensity, control,
  (select value from sample_attr b where a.sample_id=b.sample_id and name='media') as attr_media,
  (select value from sample_attr b where a.sample_id=b.sample_id and name='time') as attr_time
from mtab_sample_intensity a
where

-- mz/rt clause
abs(a.rt - 100) <= 30
and abs(1e6 * (116 - a.mz) / a.mz) <= 0.5

),
-- second subquery: compute average intensity in each sample attribute grouping
sq2 as (
select mtab_id, control, attr_media, attr_time, avg(intensity) as avg_intensity from sq1
group by mtab_id, control, attr_media, attr_time
),
-- third subquery: collate first subquery with second subquery, selecting average intensity where control=1
-- to generate "intensity in controls" column (iic) that is grouped by sample attribute
sq3 as (
select exp_id, sq1.mtab_id, mz, rt, withms2, sample_id, intensity, sq1.control, sq1.attr_media, sq1.attr_time
, (select avg_intensity from sq2
   where sq1.mtab_id=sq2.mtab_id
   and sq1.attr_media=sq2.attr_media
   and sq1.attr_time=sq2.attr_time
   and sq2.control=1) as iic
from sq1
)
-- query: filter third subquery by intensity over controls
select * from sq3 where intensity > 10 * iic
order by mtab_id, attr_media, attr_time, sample_id, control, iic
