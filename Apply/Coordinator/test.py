import operator
import json
import numpy as np
import threading
import copy
import time

a = [(1,10),(1,9),(1,8),(2,10), (2,9), (2,8), (3,10), (3,9), (3,8)]
b = [(1,2),(3,4)]
a = a + b
print(time.time())
e = threading.Event()
e.set()
e.clear()
e.wait()
a = 0
print(time.time())