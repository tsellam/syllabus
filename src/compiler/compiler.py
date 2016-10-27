class Expr(object):
    def __init__(self, op, l, r):
        self.op = op
        self.l = l
        self.r = r

    def __call__(self, row):
        if self.op == "=":
            return self.l(row) == self.r(row)
        if self.op == "<":
            return self.l(row) < self.r(row)
    
    def compile(self):
        op = None
        if self.op == "=":
            op = "=="
        elif self.op == "<":
            op = "<"
        return "(%s) %s (%s)" % (self.l.compile(), op, self.r.compile())
    
class Const(Expr):
    def __init__(self, v):
        self.v = v
        
    def __call__(self, row):
        return self.v
    
    def compile(self):
        if isinstance(self.v, basestring):
            return "'%s'" % self.v
        return str(self.v)

class Var(Expr):
    def __init__(self, attr):
        self.attr = attr
        
    def __call__(self, row):
        return row[self.attr]
    
    def compile(self):
        return "row.get('%s', None)" % self.attr
        
class Filter(object):
    def __init__(self, exprs):
        self.exprs = exprs
        
    def __call__(self, rows):
        for row in rows:
            if all(e(row) for e in self.exprs):
                yield row
    
    def compile(self):
        exprs_str = " and ".join([e.compile() for e in self.exprs])
        
        code = """
def filter_func(rows):
    for row in rows:
        if %s:
            yield row
        """ % exprs_str
        
        exec code
        print code
        return filter_func
    

if __name__ == "__main__":
  import timeit

  data = [{c: i for c in "aoeuidhtnqjkvwmb"} for i in xrange(10000)]
  e = Expr("=", Var("a"), Const(1))
  f = Filter([e])

  print timeit.timeit(lambda: len(list(f(data))), number=100)


  func = f.compile()
  print timeit.timeit(lambda: len(list(func(data))), number=100)