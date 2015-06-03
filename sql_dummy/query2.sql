select exp_id, mtab_id, mz, rt, withms2, sample_id, intensity from mtab_sample_intensity
where

-- mz/rt query
abs(1e6 * (100 - mz) / mz) <= 0.5
and abs(rt - 90) <= 30

-- exclude zero intensity
and intensity > 0
