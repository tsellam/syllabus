from pyparsing import *

import re
import math

"""
+-NOT expr
(expr)
expr op expr
f(args)
'str'
num
float
"""

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
"""
    foo = (value binaryop expr) /
               (value ("is null" / "is not null")) /
               (value IS NOT? expr) /
               (unaryop expr) /
               (value)

"""
grammar = Grammar(
    r"""
    expr     = biexpr / unexpr / value
    biexpr   = value ws binaryop ws expr
    unexpr   = unaryop expr
    value    = number /
               boolean /
               function /
               string /
               attr
    parenval = ("(" ws expr ws ")") 
    function = fname "(" ws arg_list? ws ")"
    arg_list = expr (ws "," ws expr)*
    number   = ~"\d*\.?\d+"i
    string   = ~"\'\w*\'"i
    attr     = ~"\w[\w\d]*"i
    fname    = ~"\w[\w\d]*"i
    boolean  = "true" / "false"
    binaryop = "+" / "-" / "*" / "/" / "=" / "<>" /
               "<=" / ">" / "<" / ">" / "and" / "or"
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
  def __init__(self, name, args):
    self.name = name
    self.args = args
  def __str__(self):
    args = ",".join(map(str, self.args))
    return "%s(%s)" % (self.name, args)
  def __call__(self, tup, tup2=None):
    args = [arg(tup, tup2) for arg in self.args]
    f = None
    if self.name in tup:
      f = tup[self.name]
    elif tup2 and self.name in tup2:
      f = tup2[self.name]
    else:
      raise Exception("couldn't find function %s in tuple" % self.name)
    return f(args)

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

class Visitor(NodeVisitor):
  grammar = grammar
  def visit_attr(self, node, children):
    return Attr(node.text)

  def visit_binaryop(self, node, children):
    return node.text

  def visit_biexpr(self, node, children):
    return Expr(children[2], children[0], children[-1])
  def visit_unexpr(self, node, children):
      return Expr(children[0], children[1])

  def visit_expr(self, node, children):
    return children[0]

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
