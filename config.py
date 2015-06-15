import re

def get_default_config():
    return dict(
        ppm_diff = 0.5,
        rt_diff = 30,
        with_ms2 = False,
        exclude_controls = True,
        int_over_controls = 0,
        attrs = set()
    )

def str2bool(s):
    return s in ['True','true','T','t','1','Yes','yes','Y','y']

def attrs2list(s):
    return re.split(r', *',s)

CONFIG_CASTS = dict(
    ppm_diff=float,
    rt_diff=int,
    with_ms2=str2bool,
    exclude_controls=str2bool,
    int_over_controls=float,
    attrs=attrs2list
)

def complete_config_key(config,text):
    return [k for k in config.keys() if k.startswith(text)]

def set_config_key(config,k,v):
    """v is expected to be a string"""
    config[k] = CONFIG_CASTS[k](v)

