from collections import *
try:
  import instabase.notebook.ipython.utils as ib
  ib.import_pyfile('./interpretor.py', 'interpretor')
except:
  pass
from interpretor import *

if __name__ == "__main__":
  s = Scan("./data.csv")
  f = Filter(s, "a > 3.0 and d <= 4.0")
  run_op(Print(f))

