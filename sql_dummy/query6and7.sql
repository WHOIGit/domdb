select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control
from mtab_sample_intensity a
where

-- mz/rt clause
abs(a.rt - 80) <= 30
and abs(1e6 * (105 - a.mz) / a.mz) <= 0.5

-- int_over_controls=10
and intensity > 10 * (select avg(intensity) from mtab_sample_intensity b
  where a.mtab_id=b.mtab_id
  and control=1)

-- exclude zero-intensity samples
and intensity > 0
 
order by exp_id, mtab_id, sample_id
