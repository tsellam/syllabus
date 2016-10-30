import re
import math
import numpy as np

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


grammar = Grammar(
    r"""
    query    = select_cores orderby? limit?
    select_cores   = select_core (compound_op select_core)*
    select_core    = SELECT wsp select_results from_clause? where_clause? gb_clause?
    select_results = select_result (ws "," ws select_result)*
    select_result  = sel_res_all_star / sel_res_tab_star / sel_res_val / sel_res_col 
    sel_res_tab_star = name ".*"
    sel_res_all_star = "*"
    sel_res_val    = expr (AS wsp name)?
    sel_res_col    = col_ref (AS wsp name)

    from_clause    = FROM join_source
    join_source    = ws single_source (ws "," ws single_source)*
    single_source  = source_table / source_subq
    source_table   = table_name (AS wsp name)?
    source_subq    = "(" ws query ws ")" (AS ws name)?

    where_clause   = WHERE wsp expr (AND expr)*

    gb_clause      = GROUP BY group_clause having_clause?
    group_clause   = grouping_term (ws "," grouping_term)*
    grouping_term  = ws expr
    having_clause  = HAVING expr

    orderby        = ORDER BY ordering_term (ws "," ordering_term)*
    ordering_term  = ws expr (ASC/DESC)?

    limit          = LIMIT expr (OFFSET expr)?

    col_ref        = (table_name ".")? column_name



    expr     = biexpr / unexpr / value
    biexpr   = value ws binaryop ws expr
    unexpr   = unaryop expr
    value    = parenval / 
               number /
               boolean /
               col_ref /
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
    compound_op = "UNION" / "union"
    binaryop = "+" / "-" / "*" / "/" / "=" / "<>" /
               "<=" / ">" / "<" / ">" / "and" / "or"
    unaryop  = "+" / "-" / "not"
    ws       = ~"\s*"i
    wsp      = ~"\s+"i

    name       = ~"[a-zA-Z]\w*"i
    table_name = name
    column_name = name

    ADD = wsp "ADD"
    ALL = wsp "ALL"
    ALTER = wsp "ALTER"
    AND = wsp "AND"
    AS = wsp "AS"
    ASC = wsp "ASC"
    BETWEEN = wsp "BETWEEN"
    BY = wsp "BY"
    CAST = wsp "CAST"
    COLUMN = wsp "COLUMN"
    DESC = wsp "DESC"
    DISTINCT = wsp "DISTINCT"
    E = "E"
    ESCAPE = wsp "ESCAPE"
    EXCEPT = wsp "EXCEPT"
    EXISTS = wsp "EXISTS"
    EXPLAIN = ws "EXPLAIN"
    EVENT = ws "EVENT"
    FORALL = wsp "FORALL"
    FROM = wsp "FROM"
    GLOB = wsp "GLOB"
    GROUP = wsp "GROUP"
    HAVING = wsp "HAVING"
    IN = wsp "IN"
    INNER = wsp "INNER"
    INSERT = ws "INSERT"
    INTERSECT = wsp "INTERSECT"
    INTO = wsp "INTO"
    IS = wsp "IS"
    ISNULL = wsp "ISNULL"
    JOIN = wsp "JOIN"
    KEY = wsp "KEY"
    LEFT = wsp "LEFT"
    LIKE = wsp "LIKE"
    LIMIT = wsp "LIMIT"
    MATCH = wsp "MATCH"
    NO = wsp "NO"
    NOT = wsp "NOT"
    NOTNULL = wsp "NOTNULL"
    NULL = wsp "NULL"
    OF = wsp "OF"
    OFFSET = wsp "OFFSET"
    ON = wsp "ON"
    OR = wsp "OR"
    ORDER = wsp "ORDER"
    OUTER = wsp "OUTER"
    PRIMARY = wsp "PRIMARY"
    QUERY = wsp "QUERY"
    RAISE = wsp "RAISE"
    REFERENCES = wsp "REFERENCES"
    REGEXP = wsp "REGEXP"
    RENAME = wsp "RENAME"
    REPLACE = ws "REPLACE"
    RETURN = wsp "RETURN"
    ROW = wsp "ROW"
    SAVEPOINT = wsp "SAVEPOINT"
    SELECT = ws "SELECT"
    SET = wsp "SET"
    TABLE = wsp "TABLE"
    TEMP = wsp "TEMP"
    TEMPORARY = wsp "TEMPORARY"
    THEN = wsp "THEN"
    TO = wsp "TO"
    UNION = wsp "UNION"
    USING = wsp "USING"
    VALUES = wsp "VALUES"
    VIRTUAL = wsp "VIRTUAL"
    WITH = wsp "WITH"
    WHERE = wsp "WHERE"
    """
)

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


class Visitor(NodeVisitor):
  """
  Each expression of the form

      XXX = ....
  
  in the grammar can be handled with a custom function by writing 
  
      def visit_XXX(self, node, children):

  You can assume the elements in children are the handled 
  versions of the corresponding child nodes
  """
  grammar = grammar

  def visit_query(self, node, children):
    nodes = filter(bool, children[1:])
    ret = children[0]
    for n in nodes:
      n.p = ret
      ret = n
    return ret

  def visit_select_cores(self, node, children):
    l = filter(bool, children[1])
    if len(l):
      raise Exception("We don't support multiple SELECT cores")
    return children[0]

  def visit_select_core(self, node, children):
    selectc, fromc, wherec, gbc = tuple(children[2:])
    nodes = [fromc, wherec, gbc, selectc]
    ret = None
    for n in filter(bool, nodes):
      if not ret: 
        ret = n
      else:
        n.p = ret
        ret = n
    return ret

  def visit_select_results(self, node, children):
    allexprs = [children[0]]
    allexprs.extend(filter(bool, children[1]))
    exprs = []
    aliases = []

    for i, e in enumerate(allexprs):
      if isinstance(e, tuple):
        e, alias = e
      else:
        alias = "attr%s" % i
      exprs.append(e)
      aliases.append(alias)

    return Project(None, exprs, aliases)



  def visit_sel_res_tab_star(self, node, children):
    return Star(children[0])

  def visit_sel_res_all_star(self, node, children):
    return Star()

  def visit_sel_res_val(self, node, children):
    alias = None 
    if children[1]:
      alias = children[1][2]
    return (children[0], alias)
  def visit_sel_res_col(self, node, children):
    alias = None 
    if children[1]:
      alias = children[1][2]
    return (children[0], alias)

  def visit_from_clause(self, node, children):
    pass
  def visit_join_source(self, node, children):
    pass
  def visit_single_source(self, node, children):
    pass
  def visit_source_table(self, node, children):
    pass
  def visit_source_subq(self, node, children):
    pass
  def visit_where_clause(self, node, children):
    pass
  def visit_gb_clause(self, node, children):
    pass
  def visit_group_clause(self, node, children):
    pass
  def visit_grouping_term(self, node, children):
    pass
  def visit_having_clause(self, node, children):
    pass
  def visit_orderby(self, node, children):
    pass
  def visit_ordering_term(self, node, children):
    pass
  def visit_limit(self, node, children):
    pass
  def visit_col_ref(self, node, children):
    table = children[0]
    if table:
      table = table[0]
    return Attr(children[1], table)

  def visit_name(self, node, children):
    return node.text


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
    children = filter(lambda v: v and (not isinstance(v, basestring) or v.strip()), children)
    if len(children) == 1: 
      return children[0]
    return children


def parse(s):
  return Visitor().parse(s)

if __name__ == "__main__":
  import click

  @click.command()
  @click.option("-c", type=str)
  def run(c="(a+a) > 3"):
    print c
    ast = parse(c)
    print "printing ast"
    print ast
    print ast(dict(a=2))

  run()
