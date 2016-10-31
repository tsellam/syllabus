from collections import *
try:
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./interpretor.py', 'interpretor')
  ib.import_pyfile('./optimizer.py', 'optimizer')
except:
  pass
from interpretor import *
from optimizer import *


def cond_to_func(expr_or_func):
  """
  if expr_or_func is a string, parse as an expression
  otherwise it better be a function!
  """
  if hasattr(expr_or_func, "__call__"):
    print expr_or_func
    return expr_or_func
  if isinstance(expr_or_func, basestring):
    return parseexpr(expr_or_func)
  raise Exception("Can't interpret as expression: %s" % expr_or_func)



if __name__ == "__main__":

  def test1():
    print "test1"
    o = Print(
          Limit(
            Project(
              Filter(
                Join(
                  Scan("data.csv"), 
                  Project(Scan("data.csv"), map(cond_to_func, ["a", "b", "c"]), ["x", "y", "z"]),
                  cond_to_func("a = x")),
                cond_to_func("a <= x")),
              map(cond_to_func, ["a*2", "c-a"])
            ),
            5
          )
        )
    run_op(o)

  def test2():
    print "test2"
    s = Scan("data.csv")
    g = GroupBy(s, map(cond_to_func, ["a"]))
    p = Project(g, map(cond_to_func, ["avg(b)", "avg(a)", "a"]), ["avg", "avg2", "a"])
    print p
    run_op(Print(p))

  def test3():
    # doesn't run because interpretor doesn't understand From
    print "test3"
    q = """SELECT avg(t.b) AS avg
           FROM data AS t, data AS b, c AS c, d AS d, e AS e
           WHERE t.a = b.a AND (t.a + b.a) = t.a AND
                 (t.a + d.a) = b.a AND c.a = d.a"""
    o = parse(q)
    optimize(o)
    #run_op(Print(o))

  test3()
