import re
import math
import numpy as np
from ops import *

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor


exprgrammar = Grammar(
    r"""
    expr     = biexpr / unexpr / value
    biexpr   = value ws binaryop ws expr
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
    compound_op = "UNION" / "union"
    binaryop = "+" / "-" / "*" / "/" / "=" / "<>" /
               "<=" / ">" / "<" / ">" / "and" / "or"
    unaryop  = "+" / "-" / "not"
    ws       = ~"\s*"i
    wsp      = ~"\s+"i
    """)



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

    limit          = LIMIT wsp expr (OFFSET expr)?

    col_ref        = (table_name ".")? column_name



    expr     = biexpr / unexpr / value
    biexpr   = value ws binaryop ws expr
    unexpr   = unaryop expr
    value    = parenval / 
               number /
               boolean /
               function /
               col_ref /
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

def flatten(children, sidx, lidx):
  ret = [children[sidx]]
  rest = children[lidx]
  if not isinstance(rest, list): rest = [rest]
  ret.extend(filter(bool, rest))
  return ret


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


  #
  #  SELECT CLAUSE
  #

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
    allexprs = flatten(children, 0, 1)
    exprs = []
    aliases = []

    for i, e in enumerate(allexprs):
      if isinstance(e, tuple):
        e, alias = e
        if not alias:
          alias = "attr%s" % i
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
      alias = children[1]
    return (children[0], alias)

  def visit_sel_res_col(self, node, children):
    alias = None 
    if children[1]:
      alias = children[1]
    return (children[0], alias)


  #
  # FROM CLAUSE
  #

  def visit_from_clause(self, node, children):
    return children[1]

  def visit_join_source(self, node, children):
    sources = flatten(children, 1, 2)
    return From(sources)

  def visit_source_table(self, node, children):
    tname = children[0]
    alias = tname
    if children[1]:
      alias = children[1][1]
    return Scan(tname, alias)

  def visit_source_subq(self, node, children):
    subq = children[2]
    alias = children[5] 
    return SubQuerySource(subq, alias)


  #
  # Other clauses
  #

  def visit_where_clause(self, node, children):
    exprs = flatten(children, 2, -1)
    ret = exprs[0]
    for e in exprs[1:]:
      ret = Expr("and", e, ret)
    return Filter(None, ret)

  def visit_gb_clause(self, node, children):
    gb = children[2] 
    having = children[3]
    if having:
      having.p = gb
      return having
    return gb

  def visit_group_clause(self, node, children):
    groups = flatten(children, 0, 1)
    return GroupBy(None, groups)

  def visit_grouping_term(self, node, children):
    return children[1]

  def visit_having_clause(self, node, children):
    return children[1]

  def visit_orderby(self, node, children):
    terms = flatten(children, 2, 3)
    exprs, ascdesc = zip(*terms)
    return OrderBy(None, exprs, ascdesc)

  def visit_ordering_term(self, node, children):
    expr = children[1]
    order = children[2] or "asc"
    return (expr, order)

  def visit_limit(self, node, children):
    if children[3]:
      print "WARN: don't support offset yet"
    return Limit(None, children[2])

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
    return flatten(children, 0, 1)
  
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

class ExprVisitor(Visitor):
  grammar = exprgrammar

def parse(s):
  return Visitor().parse(s)

def parseexpr(s):
  return ExprVisitor().parse(s)

if __name__ == "__main__":
  import click

  @click.command()
  @click.option("-c", type=str)
  def run(c="(a+a) > 3"):
    print c
    ast = parse(c)
    print "printing ast"
    print ast

  run()
