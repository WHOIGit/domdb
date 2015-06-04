select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control
from mtab_sample_intensity a
where

-- mz/rt clause
abs(a.rt - 100) <= 30
and abs(1e6 * (103 - a.mz) / a.mz) <= 0.5

-- exclude mtabs that occur in controls
and (select max((control=1)::int) from mtab_sample_intensity b
  where a.mtab_id=b.mtab_id
  and intensity > 0) = 0

-- exclude zero-intensity samples
and intensity > 0
 
order by exp_id, mtab_id, sample_id
