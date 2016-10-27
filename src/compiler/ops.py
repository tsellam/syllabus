import csv
import math
import numpy as np



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

class From(Op):
  def __init__(self, sources):
    self.sources = sources

  def __str__(self):
    arg = ",\n".join(["\t%s" % s for s in self.sources])
    return "FROM:\n%s" % arg

class Source(Op):
  pass

class SubQuerySource(Source):
  def __init__(self, p, alias=None):
    self.p = p
    self.alias = alias 

  def __str__(self):
    return "Source: (%s)" % self.p

class Scan(Op):
  def __init__(self, filename, alias=None):
    if "." not in filename:
      filename += ".csv"

    self.filename = filename
    self.alias = alias or filename
    self.fields = []
    self.types = []
    self.data = []

    try:
      openfile = open
      try:
        openfile = ib.open
      except:
        pass

      with openfile(self.filename) as f:
          self.proc_file(f)
    except Exception as e:
      print e

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
  
  def __str__(self):
    return "Source: %s" % self.filename

      
class Join(Op):
  """
  Theta Join
  """
  def __init__(self, l, r, cond=lambda (t1, t2): True):
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
    self.cond = cond

  def __str__(self):
    return "JOIN: %s\t%s ON %s" % (str(self.l), str(self.r), str(self.cond))


class GroupBy(Op):
  def __init__(self, p, group_exprs):
    """
    @p           parent operator
    @group_exprs list of functions that take the tuple as input and
                 outputs a scalar value
    """
    self.p = p
    self.group_exprs = group_exprs

  def __str__(self):
    return "GROUP BY: %s\n%s" % (",".join(map(str, self.group_exprs)), self.p)

class OrderBy(Op):
  def __init__(self, p, order_exprs, ascdesc):
    """
    @p            parent operator
    @order_exprs  ordered list of function that take the tuple as input 
                  and outputs a scalar value
    """
    self.p = p
    self.order_exprs = order_exprs
    self.ascdesc = ascdesc or []
    for i in xrange(len(order_exprs)):
      if len(self.ascdesc) < i:
        self.ascdesc.append("asc")


  def __str__(self):
    return "ORDER BY: %s\n%s" % (",".join(map(str, self.order_exprs)), self.p)


class Filter(Op):
  def __init__(self, p, cond):
    """
    @p            parent operator
    @cond         boolean function that takes a tuple as input
    """
    self.p = p
    self.cond = cond

  def __str__(self):
    return "WHERE: %s\n%s" % (str(self.cond), self.p)

class Limit(Op):
  def __init__(self, p, limit):
    """
    @p            parent operator
    @limit        number of tuples to return
    """
    self.p = p
    self.limit = limit

  def __str__(self):
    return "LIMIT: %s\n%s" % (self.limit,  self.p)


class Project(Op):
  def __init__(self, p, exprs, aliases=None):
    """
    @p            parent operator
    @exprs        list of function that take the tuple as input and
                  outputs a scalar value
    @aliases      name of the fields defined by the above exprs
    """
    self.p = p
    self.exprs = exprs
    self.aliases = aliases or []
    if len(self.aliases) < len(self.exprs):
      self.aliases.extend(
          ["attr%s" % i for i in range(len(self.exprs)-len(self.aliases))])


  def __str__(self):
    args = ", ".join(["%s AS %s" % (e, a) for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project: %s\n%s" % (args, str(self.p))





def unary(op, v):
  if op == "+":
    return v
  if op == "-":
    return -v
  if op.lower() == "not":
    return not(v)

def binary(op, l, r):
  if op == "+": return l + r
  if op == "/": return l / r
  if op == "*": return l * r
  if op == "-": return l - r
  if op == "=": return l == r
  if op == "<>": return l != r
  if op == "and": return l and r
  if op == "or": return l or r
  if op == "<": return l < r
  if op == ">": return l > r
  if op == "<=": return l <= r
  if op == ">=": return l >= r
  return True

class Expr(object):
  def __init__(self, op, l, r=None):
    self.op = op
    self.l = l
    self.r = r

  def __str__(self):
    if self.r:
      return "%s %s %s" % (self.l, self.op, self.r)
    return "%s %s" % (self.op, self.l)

  def __call__(self, tup, tup2=None):
    l = self.l(tup, tup2)
    if self.r is None:
      return unary(self.op, l)
    r = self.r(tup, tup2)
    return binary(self.op, l, r)

class Func(object): 
  """
  This object needs to deal with scalar AND aggregation functions.
  """
  agg_func_lookup = dict(
    avg=np.mean,
    count=len,
    sum=np.sum,
    std=np.std,
    stddev=np.std
  )
  scalar_func_lookup = dict(
    lower=lambda s: str(s).lower()
  )


  def __init__(self, name, args):
    self.name = name.lower()
    self.args = args

  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)

  def __call__(self, tup, tup2=None):
    f = Func.agg_func_lookup.get(self.name, None)
    if f:
      if "__group__" not in tup:
        raise Exception("aggregation function %s called but input is not a group!")
      args = []
      for gtup in tup["__group__"]:
        args.append([arg(gtup) for arg in self.args])

      # make the arguments columnar:
      # [ (a,a,a,a), (b,b,b,b) ]
      args = zip(*args)
      return f(*args)


    f = agg_func_lookup.get(self.name, None)
    if f:
      args = [arg(tup, tup2) for arg in self.args]
      return f(args)

    raise Exception("I don't recognize function %s" % self.name)

class Literal(object):
  def __init__(self, v):
    self.v = v
  def __call__(self, tup=None, tup2=None): 
    return self.v
  def __str__(self):
    if isinstance(self.v, basestring):
      return "'%s'" % self.v
    return str(self.v)

class Attr(object):
  def __init__(self, attr, tablename=None):
    self.attr = attr
    self.tablename = tablename
    if self.tablename:
      print "WARNING: can't deal with * for specific tables: %s" % self.tablename

  def __call__(self, tup, tup2=None):
    if self.attr in tup:
      return tup[self.attr]
    if tup2 and self.attr in tup2:
      return tup2[self.attr]
    raise Exception("couldn't find %s in either tuple" % self.attr)

  def __str__(self):
    return self.attr

class Star(object):
  def __init__(self, tablename=None):
    self.tablename = tablename
    if self.tablename:
      print "WARNING: can't deal with * for specific tables: %s" % self.tablename

  def __call__(self, tup, tup2=None):
    return tup

  def __str__(self):
    if self.tablename:
      return "%s.*" % self.tablename
    return "*"


