from collections import *
try:
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./ops.py', 'ops')
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./parser.py', 'parser')

except:
  pass
from ops import *
from parser import parse, parseexpr

def run_op(op, f=lambda t:t):
  """
  This function interprets the current operator and constructs an 
  appropriate callback function to send to the parent operator

  @op current operator to execute
  @f the function to call for every output tuple of this operator (op)
  """
  klass = op.__class__.__name__

  if klass == "Print":
    def print_f(tup):
      print tup
    run_op(op.c, print_f)

  elif klass == "Scan":
    for tup in op.data:
      if f(tup) == False:
        break

  elif klass == "Join":
    def outer_loop(left):
      def inner_loop(right):
        if op.cond(left, right):
          newtup = dict()
          newtup.update(left)
          newtup.update(right)
          f(newtup)
      run_op(op.r, inner_loop)
    run_op(op.l, outer_loop)

  elif klass == "Limit":
    # super ugly object hack because int counter doesn't work
    class I(object):
      def __init__(self):
        self.i = 0
    def __f__(i):
      def limit_f(tup):
        if i.i >= op.limit:
          return False
        i.i += 1
        f(tup)
      return limit_f
    run_op(op.c, __f__(I()))

  elif klass == "GroupBy":
    hashtable = defaultdict(lambda: [None, None, []])
    def group_f(tup):
      key = tuple([e(tup) for e in op.group_exprs])
      hashtable[key][0] = key
      hashtable[key][1] = tup
      hashtable[key][2].append(tup)
    run_op(op.c, group_f)

    for _, (key, tup, group) in hashtable.iteritems():
      tup = dict(tup)
      tup["__key__"] = key
      tup["__group__"] = group
      f(tup)

  elif klass == "OrderBy":
    tup_buffer = []
    def order_f(tup):
      tup_buffer.append(tup)
    run_op(op.c, order_f)

  elif klass == "Filter":
    def where_f(tup):
      if op.cond(tup):
        f(tup)
    run_op(op.c, where_f)

  elif klass == "Project":
    def project_f(tup):
      ret = dict()
      for exp, alias in zip(op.exprs, op.aliases):
        ret[alias] = exp(tup)
      f(ret)
    run_op(op.c, project_f)




