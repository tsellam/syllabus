import csv
try:
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./parser.py', 'parser')
except:
  pass
from parser import parse

def cond_to_func(expr_or_func):
  """
  if expr_or_func is a string, parse as an expression
  otherwise it better be a function!
  """
  if hasattr(expr_or_func, "__call__"):
    print expr_or_func
    return expr_or_func
  if isinstance(expr_or_func, basestring):
    return parse(expr_or_func)
  raise Exception("Can't interpret as expression: %s" % expr_or_func)


class Op:
  """
  Base class
  """
  def __init__(self):
    pass

  def next(self):
    pass

class Print(Op):
  def __init__(self, p):
    self.p = p

class Scan(Op):
  def __init__(self, filename):
    openfile = open
    try:
      openfile = ib.open
    except:
      pass

    with openfile(filename) as f:
        self.proc_file(f)

  def proc_file(self, f):
    fields = None
    types = None
    rows = None
    dialect = csv.Sniffer().sniff(f.read(2048))
    f.seek(0)
    reader = csv.reader(f, dialect)
    header = reader.next()
    header = [v.split(":") for v in header]
    fields, types = zip(*header)
    rows = [dict(zip(fields, l)) for l in reader]
    for row in rows:
      for f,t in zip(fields, types):
        if t == "num":
          row[f] = float(row[f])
    self.fields = fields
    self.types = types
    self.data = rows

      
class Join(Op):
  """
  Theta Join
  """
  def __init__(self, l, r, cond="true"):
    """
    @l    left (outer) table of the join
    @r    right (inner) table of the join
    @cond a boolean function that takes as input two tuples, 
          one from the left table, one from the right
          OR
          an expression
    """
    self.l = l
    self.r = r
    self.cond = cond_to_func(cond) 


class GroupBy(Op):
  def __init__(self, p, group_exprs):
    """
    @p           parent operator
    @group_exprs list of functions that take the tuple as input and
                 outputs a scalar value
    """
    self.p = p
    self.group_exprs = map(cond_to_func, group_exprs)

class OrderBy(Op):
  def __init__(self, p, order_exprs):
    """
    @p            parent operator
    @order_exprs  ordered list of function that take the tuple as input 
                  and outputs a scalar value
    """
    self.p = p
    self.order_exprs = map(cond_to_func, order_exprs)

class Filter(Op):
  def __init__(self, p, cond):
    """
    @p            parent operator
    @cond         boolean function that takes a tuple as input
    """
    self.p = p
    self.cond = cond_to_func(cond)

class Limit(Op):
  def __init__(self, p, limit):
    """
    @p            parent operator
    @limit        number of tuples to return
    """
    self.p = p
    self.limit = limit


class Project(Op):
  def __init__(self, p, exprs, aliases=None):
    """
    @p            parent operator
    @exprs        list of function that take the tuple as input and
                  outputs a scalar value
    @aliases      name of the fields defined by the above exprs
    """
    self.p = p
    self.exprs = map(cond_to_func, exprs)
    self.aliases = aliases or []
    if len(self.aliases) < len(self.exprs):
      self.aliases.extend(
          ["attr%s" % i for i in range(len(self.exprs)-len(self.aliases))])



