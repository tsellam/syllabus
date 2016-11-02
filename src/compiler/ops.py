import csv
import math
import numpy as np
import inspect
import types



class Op(object):
  """
  Base class

  all operators have a single parent
  an operator may have multiple children
  """
  def __init__(self):
    self.p = None

  def replace(self, newop):
    if not self.p: return
    p = self.p
    if isinstance(p, UnaryOp):
      p.c = newop
    if isinstance(p, BinaryOp):
      if p.l == self:
        p.l = newop
      elif p.r == self:
        p.r = newop
    if isinstance(p, NaryOp):
      if self in p.cs:
        cs[cs.index(self)] = newop
      p.cs = cs

  def is_ancestor(self, anc):
    n = self
    seen = set()
    while n and n not in seen:
      seen.add(n)
      if n == anc:
        return True
      n = n.p
    return False

  def children(self):
    """
    Go through all attributes of this object and return those that
    are subclasses of Op
    """
    children = []
    for key, attrval in self.__dict__.iteritems():
      if key == "p": 
        continue
      if not isinstance(attrval, list):
        attrval = [attrval]
      for v in attrval:
        if v and isinstance(v, Op):
          children.append(v)
    return children

  def traverse(self, f):
    f(self)
    for child in self.children():
      child.traverse(f)

  def collect(self, klass_or_names):
    """
    Collect operators with the same class name as argument
    """
    ret = []
    if not isinstance(klass_or_names, list):
      klass_or_names = [klass_or_names]
    names = [kn for kn in klass_or_names if isinstance(kn, basestring)]
    klasses = [kn for kn in klass_or_names if isinstance(kn, type)]

    def f(node):
      if node and (
          node.__class__.__name__ in names or
          any([isinstance(node, kn) for kn in klasses])):
        ret.append(node)
    self.traverse(f)
    return ret

  def collectone(self, klassnames):
    l = self.collect(klassnames)
    if l:
      return l[0]
    return None


class UnaryOp(Op):
  def __init__(self, c):
    super(UnaryOp, self).__init__()
    self.c = c
    if c:
      c.p = self

  def __setattr__(self, attr, v):
    super(UnaryOp, self).__setattr__(attr, v)
    if attr == "c" and v:
      self.c.p = self
 
class BinaryOp(Op):
  def __init__(self, l, r):
    super(BinaryOp, self).__init__()
    self.l = l
    self.r = r
    if l:
      l.p = self
    if r:
      r.p = self

  def __setattr__(self, attr, v):
    super(BinaryOp, self).__setattr__(attr, v)
    if attr in ("l", "r") and v:
      v.p = self
   
class NaryOp(Op):
  def __init__(self, cs):
    super(NaryOp, self).__init__()
    self.cs = cs
    for c in cs:
      if c:
        c.p = self

  def __setattr__(self, attr, v):
    super(NaryOp, self).__setattr__(attr, v)
    if attr == "cs":
      for c in self.cs:
        c.p = self
 
class Print(UnaryOp):
  pass

class From(NaryOp):
  def __str__(self):
    arg = ",\n".join(["\t%s" % s for s in self.cs])
    return "FROM:\n%s" % arg

class Source(UnaryOp):
  pass

class SubQuerySource(Source):
  def __init__(self, c, alias=None):
    super(SubQuerySource, self).__init__(c)
    self.alias = alias 

  def __str__(self):
    return "Source: (%s AS %s)" % (self.c, self.alias)

class Scan(Source):
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
    return "Source:(%s AS %s)" % (self.filename, self.alias)

      
class Join(BinaryOp):
  """
  Theta Join
  """
  def __init__(self, l, r, cond=None):
    """
    @l    left (outer) table of the join
    @r    right (inner) table of the join
    @cond a boolean function that takes as input two tuples, 
          one from the left table, one from the right
          OR
          an expression
    """
    super(Join, self).__init__(l, r)
    self.cond = cond or Bool(True)

  def __str__(self):
    return "JOIN:(\n\t%s\n\t%s ON %s)" % (str(self.l), str(self.r), str(self.cond))


class GroupBy(UnaryOp):
  def __init__(self, c, group_exprs):
    """
    @p           parent operator
    @group_exprs list of functions that take the tuple as input and
                 outputs a scalar value
    """
    super(GroupBy, self).__init__(c)
    self.group_exprs = group_exprs

  def __str__(self):
    return "GROUP BY: %s\n%s" % (",".join(map(str, self.group_exprs)), self.c)


class OrderBy(UnaryOp):
  def __init__(self, c, order_exprs, ascdesc):
    """
    @p            parent operator
    @order_exprs  ordered list of function that take the tuple as input 
                  and outputs a scalar value
    """
    super(OrderBy, self).__init__(c)
    self.order_exprs = order_exprs
    self.ascdesc = ascdesc or []
    for i in xrange(len(order_exprs)):
      if len(self.ascdesc) < i:
        self.ascdesc.append("asc")


  def __str__(self):
    return "ORDER BY: %s\n%s" % (",".join(map(str, self.order_exprs)), self.c)



class Filter(UnaryOp):
  def __init__(self, c, cond):
    """
    @p            parent operator
    @cond         boolean function that takes a tuple as input
    """
    super(Filter, self).__init__(c)
    self.cond = cond

  def __str__(self):
    return "WHERE: %s\n%s" % (str(self.cond), self.c)


class Limit(UnaryOp):
  def __init__(self, c, limit):
    """
    @p            parent operator
    @limit        number of tuples to return
    """
    super(Limit, self).__init__(c)
    self.limit = limit

  def __str__(self):
    return "LIMIT: %s\n%s" % (self.limit,  self.c)


class Project(UnaryOp):
  def __init__(self, c, exprs, aliases=None):
    """
    @p            parent operator
    @exprs        list of function that take the tuple as input and
                  outputs a scalar value
    @aliases      name of the fields defined by the above exprs
    """
    super(Project, self).__init__(c)
    self.exprs = exprs
    self.aliases = list(aliases) or []
    self.set_default_aliases()

  def set_default_aliases(self):
    if len(filter(bool, self.aliases)) >= len(self.exprs):
      print "aliases"
      print self.aliases
      self.aliases = self.aliases[:len(self.exprs)]
      print self.exprs
      print self.aliases
      return 

    self.aliases.extend([None] * (len(self.exprs) - len(self.aliases)))
    print self.aliases
    for i in xrange(len(self.exprs)):
      if not self.aliases[i] and not isinstance(self.exprs[i], Star):
        self.aliases[i] = "attr%s" % i 


  def __str__(self):
    args = ", ".join(["%s AS %s" % (e, a) for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project: %s\n%s" % (args, str(self.c))





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


class Expr(Op):
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

class Between(Op):
  def __init__(self, expr, lower, upper):
    """
    expr BETWEEN lower AND upper
    """
    self.expr = expr
    self.lower = lower
    self.upper = upper

  def __str__(self):
    return "(%s) BETWEEN (%s) AND (%s)" % (self.expr, self.lower, self.upper)

  def __call__(self, tup, tup2=None):
    e = self.expr(tup, tup2)
    l = self.lower(tup, tup2)
    u = self.upper(tup, tup2)
    return e >= l and e <= u

class Func(Op): 
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

class Literal(Op):
  def __init__(self, v):
    self.v = v

  def __call__(self, tup=None, tup2=None): 
    return self.v

  def __str__(self):
    if isinstance(self.v, basestring):
      return "'%s'" % self.v
    return str(self.v)

class Bool(Op):
  def __init__(self, v):
    self.v = v
  def __call__(self, *args, **kwargs):
    return self.v
  def __str__(self):
    return str(self.v)

class Attr(Op):
  def __init__(self, attr, tablename=None):
    self.attr = attr
    self.tablename = tablename

  def __call__(self, tup, tup2=None):
    if self.attr in tup:
      return tup[self.attr]
    if tup2 and self.attr in tup2:
      return tup2[self.attr]
    raise Exception("couldn't find %s in either tuple" % self.attr)

  def __str__(self):
    if self.tablename:
      return "%s.%s" % (self.tablename, self.attr)
    return self.attr

class Star(Op):
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


