def rpad(s,l,pad_string=' '):
    return s + (pad_string * (l - len(s)))

def asciitable(dicts,disp_cols=None,none_msg=None,border=True):
    """produce an ASCII formatted columnar table from the dicts"""
    dicts = list(dicts)
    if not dicts:
        if none_msg is not None:
            yield none_msg
        return
    if disp_cols is not None:
        cols = disp_cols
    else:
        # set of all keys in dicts
        cols = sorted(list(set(reduce(lambda x,y: x+y, [d.keys() for d in dicts]))))
    # compute col widths. initially wide enough for the column label
    widths = dict([(col,len(col)) for col in cols])
    # now create rows, and in doing so compute max width of each column
    for row in list(dicts):
        for col in cols:
            try:
                width = len(str(row[col]))
            except KeyError:
                width = 0
            if width > widths[col]:
                widths[col] = width
    def bord(line,border_char='|',pad_char=' '):
        if border:
            return border_char + pad_char + line + pad_char + border_char
        else:
            return line
    # now print rows
    spacer = bord('-+-'.join(['-' * widths[col] for col in cols]),'+','-')
    if border:
        yield spacer
    yield bord(' | '.join([rpad(col,widths[col]) for col in cols]),'|')
    yield spacer
    for row in dicts:
        yield bord(' | '.join([rpad(str(row[col]),widths[col]) for col in cols]),'|')
    if border:
        yield spacer

def resultproxy2asciitable(r,empty_message='No rows'):
    """yields an asciitable representation of an SQLAlchemy ResultProxy"""
    cols = []
    row_proxies = r.fetchall()
    rows = []
    for r in row_proxies:
        if not cols:
            cols = r.keys()
        rows.append(dict(r.items()))
    for line in asciitable(rows,cols,empty_message):
        print line
