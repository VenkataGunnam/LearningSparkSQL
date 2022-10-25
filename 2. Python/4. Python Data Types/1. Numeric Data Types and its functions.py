# Numeric Data Types -- Int, Float, Complex,  long is no more used from python3. int itself handls the long data type
# Text type -- string
# sequence type --  list, tuple , set
# mapping type -- dict
# Boolean type -- bool
# binary types --  bytes, bytearray , memoryview

a = 10
print(type(a))

b = 10008522222222222222222222222222222222222222222222222
print(type(a))

c = 20.5454
print(type(c))

d = 3.14j
print(type(d))

# format to float type
num=10
print(num)
# converting into float to show fractions  f'{num:5f}'
num1 = f'{num:.2f}'
print(num1)

# converting
v = 25
u = "12"
w = 1
print(type(v))
print(type(u))
print(type(w))

# convert 25 to string
print(str(v))
print(type(str(v)))

# convert 12 to int
print(int(u))
print(type(int(u)))

# convert to float
print(float(25))

# fetch all builtin functions
print(dir(__builtins__))
print(dir(int))

# builtin functions
# builtin functions are available in the above
x=abs(-3.5)
print(x)
x=round(10664.0545)
print(x)
x=round(10664.0545,2)
print(x)
x=range(10,20)
print(x)
# range can be viewed by using list
x = list(range(10,20))
print(x)
x = pow(10,2)
print(x)

# mathmatical functions can be imported by using module
import math as m
x = dir(m)
print(x)

# Randome is another module which is widely used.
import random as r
x = dir(r)
print(x)

x = r.choice(range(10,20))
print(x)

x=r.random()
print(x)


y=r.randrange(10,24)
print(y)

y=r.randrange(10,24,5)
print(y)

lst = [10,20,30,40]
print(lst)
r.shuffle(lst)
print(lst)


