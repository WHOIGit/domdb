query="""select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity, control,
         (select attrs from agg_sample_attrs asa where asa.sample_id=a.sample_id)
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

ppm_diff = 0.5
rt_range = 30

mz = 102
rt = 90

params = (mz,ppm_diff,rt,rt_range)

print query % tuple(map(str,params))
