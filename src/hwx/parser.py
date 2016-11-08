import re
import math
import numpy as np

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor



grammar = Grammar(
    r"""
    expr_and = expr (and_expr)*
    and_expr = wsp andor wsp expr
    expr     = biexpr / unexpr / value
    biexpr   = value ws binaryop_no_andor ws expr
    unexpr   = unaryop expr
    value    = parenval / 
               number /
               boolean /
               function /
               string /
               attr
    parenval = "(" ws expr ws ")"
    function = fname "(" ws arg_list? ws ")"
    arg_list = expr (ws "," ws expr)*
    number   = ~"\d*\.?\d+"i
    string   = ~"\'\w*\'"i
    attr     = ~"\w[\w\d]*"i
    fname    = ~"\w[\w\d]*"i
    boolean  = "true" / "false"
    binaryop_no_andor = "+" / "-" / "*" / "/" / "=" / "<>" /
               "<=" / ">=" / "<" / ">" 
    andor    = "and" / "or"
    unaryop  = "+" / "-" / "not"
    ws       = ~"\s*"i
    wsp      = ~"\s+"i
    """
)
tree = grammar.parse("a = f(1,2) + f(1 , a)")

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
  if op == "and": return (l and r)
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
  def __init__(self, attr):
    self.attr = attr
  def __call__(self, tup, tup2=None):
    if self.attr in tup:
      return tup[self.attr]
    if tup2 and self.attr in tup2:
      return tup2[self.attr]
    raise Exception("couldn't find %s in either tuple" % self.attr)

  def __str__(self):
    return self.attr


def flatten(children, sidx, lidx):
  ret = [children[sidx]]
  rest = children[lidx]
  if not isinstance(rest, list): rest = [rest]
  ret.extend(filter(bool, rest))
  return ret


class Visitor(NodeVisitor):
  grammar = grammar

  def visit_expr_and(self, node, children):
    l = flatten(children, 0, 1)
    ret = l[0]
    for op, expr in l[1:]:
      ret = Expr(op, ret, expr)
    return ret

  def visit_and_expr(self, node, children):
    return (children[1], children[3])
 
  def visit_expr(self, node, children):
    return children[0]

  def visit_attr(self, node, children):
    return Attr(node.text)

  def visit_binaryop_no_andor(self, node, children):
    return node.text

  def visit_andor(self, node, children):
    return node.text

  def visit_biexpr(self, node, children):
    return Expr(children[2], children[0], children[-1])

  def visit_unexpr(self, node, children):
      return Expr(children[0], children[1])

  def visit_function(self, node, children):
    fname = children[0]
    arglist = children[3]
    return Func(fname, arglist)

  def visit_fname(self, node, children):
    return node.text

  def visit_arg_list(self, node, children):
    args = []
    e = children[0]
    l = filter(bool, children[1])
    args.append(e)
    args.extend(l)
    return args
  
  def visit_number(self, node, children):
    return Literal(float(node.text))

  def visit_string(self, node, children):
    return Literal(node.text)

  def visit_parenval(self, node, children):
    return children[2]

  def visit_value(self, node, children):
    return children[0]

  def visit_parenval(self, node, children):
    return children[2]

  def visit_boolean(self, node, children):
    if node.text == "true":
      return Literal(True)
    return Literal(False)

  def generic_visit(self, node, children):
    children = filter(bool, children)
    if len(children) == 1: 
      return children[0]
    return children


def parse(s):
  return Visitor().parse(s)

if __name__ == "__main__":
  print parse("a < a+1 and a > 10 and a < 9")(dict(a=2))
  print parse("a+1")(dict(a=2))

