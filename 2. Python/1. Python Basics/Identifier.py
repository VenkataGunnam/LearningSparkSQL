# '#' is used to mention the comments

# Identifier
name = 'spark'

# to print we use print function
print(name)
print('spark')

# the below line throws error as sparo is not declared
 #print(sparo)


# to enter data in multi line use " \ "
add = 1 + \
    2 + \
    3
print(1 + 2 + 3)
print (1+\
       2+\
       3)

# + is also used to concatenat two strings
print("concatenate two  + " + "strings using + symbol like this ")
# basic syntax for identification
# word or character to be in single quote

print('word')
print(" sentence can be in double quotes so you can use ' apostrophe in it like this sentence  ")
print (""" paragraph can be return in the three double quotes
which is as mentioned in this ' comment
so follow the " syntax to enter the paragraph 
so you can enter double quotes and single quotes in it like this example """)

# multi line comment can be done using 3 single apostrophe " ''' "
''' this is a multiline comment
    can be commented using three single apostrophes
    '''

# input is used to take any value as input
input("press any number ")
# 20
a = 10; b=20; c=30;
print(a)
print(b)
print(c)

# this file identifier can be called and run using python3 identifier.py
# as there are no parameters


# sys is library it have many functions in it
# importing sys we can use arguments , by default arguments are in the list format.

import sys
# the file itself will be the first argument
print(" list of arguments", sys.argv)
# to concatenate using  + symbol we need to make both of them as strings
# type can be checked using type function
print(type("list of arguments"))
print(type(sys.argv))

# the sys.argv is in list format. lets convert it into string
# will learn variables later
x = str(sys.argv)
print(x)
print(type(x))
print(" list of arguments " + x)

# can call the python file with more arguments
# python3 identifier.py arg1 arg2 arg3 ( these arg1 arg2 arg3 can be used in the file )







