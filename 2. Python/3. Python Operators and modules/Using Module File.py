import PythonModules as pm
import time as t
import sys as s
pm.Greetings('Venkat')
print(pm.Gunnam)
x = pm.Gunnam["Name"]
print(x)
y = pm.Gunnam["Id"]
print(y+250)

# all functions can be written in the module and can be used here
print(dir(t))

print(t.time())
print(t.gmtime())
 