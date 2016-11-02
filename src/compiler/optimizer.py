from ops import *
from itertools import *
from collections import *

def pickone(l, attr):
  return [(i and getattr(i, attr) or None) for i in l]

def optimize(op):
  if not op:
    return None

  while op.collectone("From"):
    op = from_expansion(op)
  print op
  return op

def from_expansion(op):
  """
  Stupid method that replaces the first From operator with a Join tree
  """
  print op
  fromop = op.collectone("From")
  filters = op.collect("Filter")
  filters = filter(lambda f: fromop.is_ancestor(f), filters)
  sources = fromop.cs


  sourcealiases = [s.alias for s in sources]
  alias2source = { s.alias: s for s in sources }

  print "filters", len(filters)
  exprs = []
  for f in filters:
    exprs.extend(f.collect(Expr))

  pairs = defaultdict(list)
  for expr in exprs:
    if expr.op == "=":
      lattrs = expr.l.collect(Attr)
      rattrs = expr.r.collect(Attr)
      names = sorted(set(pickone(chain(lattrs, rattrs), "tablename")))
      if len(names) != 2:
        continue
      if not all(a in sourcealiases for a in names):
        continue
      key = tuple(names)
      pairs[key].append(str(expr))

  print pairs

  join_order = []
  for pair, conds in pairs.iteritems():
    pair = filter(lambda name: name not in join_order, pair)
    if not pair: 
      continue
    join_order.extend(pair)

  for alias in sourcealiases:
    if alias not in join_order:
      join_order.append(alias)

  print join_order
  join_op = None
  for i, alias in enumerate(join_order):
    if not join_op:
      join_op = alias2source[alias]
    else:
      key = (join_order[i-1], join_order[i])
      if key in pairs:
        exprs = pairs[key]
        expr = exprs[0]
        for e in exprs[1:]:
          expr = Expr("and", expr, e)
        join_op = Join(join_op, alias2source[alias], expr)
      else:
        join_op = Join(join_op, alias2source[alias])


  fromop.replace(join_op)

  return op
