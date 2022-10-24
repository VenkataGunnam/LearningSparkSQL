# Global Variables which are created outside the function are called Global Variables
# Golbal variables can be used in inside function and outside function
# the variables declared inside function are called as local variables those are not used outside of the function
# casting or type is mentioned by using single column, " : int"

import sys
x: int= 10
veg1, veg2, veg3 = "Potato", "Banana", "Beams"

print(veg1)

a, b, c = 10,30,20

def my_greatest():
    print('z value is declared inside function as global so this can be used outside after this function is called')
    global z
    z = 'zebra corssing'
    if (a > b and a > c):
        print('greatest value is a i.e',a)
    elif ( b > a and b > c):
        print('greatest value is b i.e',b)
    else:
        print('greatest value is c i.e',c)
        print("z vaiable is local variable ",z)
print("values are from the outside")
my_greatest()
print('z value outside function',z)

# print statement, the values can be provided into print statement using .format

print("the value of a is {a} and value of b is {b} and value of c is {c} ".format(a=a,b=b,c=c))
print("leanring {course} is {reaction}".format(reaction='beautiful', course='Python'))

# if empty braces are given it considers the values in series
print("learning {} is {}".format('Spakr','Nice'))
p = "Hello World"
type(p)
print(type(p))
# to know what inbuilt functions/methods can be applied on the p we use dir
dir(p)
print(dir(p))
# to know what are the input to be provided full documentation of each method we can use help command
print(help(str))



