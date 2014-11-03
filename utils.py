def rpad(s,l,pad_string=' '):
    return s + (pad_string * (l - len(s)))

def asciitable(dicts,disp_cols=None):
    """produce an ASCII formatted columnar table from the dicts"""
    # set of all keys in dicts
    cols = sorted(list(set(reduce(lambda x,y: x+y, [d.keys() for d in dicts]))))
    if disp_cols is not None:
        cols = disp_cols
    # compute col widths. initially wide enough for the column label
    widths = dict([(col,len(col)) for col in cols])
    # now create rows, and in doing so compute max width of each column
    for row in dicts:
        for col in cols:
            try:
                width = len(str(row[col]))
            except KeyError:
                width = 0
            if width > widths[col]:
                widths[col] = width
    # now print rows
    yield ' | '.join([rpad(col,widths[col]) for col in cols])
    yield '-+-'.join(['-' * widths[col] for col in cols])
    for row in dicts:
        yield ' | '.join([rpad(str(row[col]),widths[col]) for col in cols])
