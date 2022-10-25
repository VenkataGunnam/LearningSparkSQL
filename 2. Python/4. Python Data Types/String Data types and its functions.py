s = "Hello world, how are you"
print(s)

s = ("hello %s developers, learning %s is great! and %d times faster than hadoop"  % ('spark','python',100))
print(s )

s = ("hello %s developers, learning %s is great! and %d times faster than hadoop" )
print(s %('python','spark',10))

x = "hello" in s
print(x)

s = "Hello world, how are you"
for i in s:
    print('......'+i+'.........')

print(s[0])
print(s[2])
print(s[0:20])
print(s[0::])
print(s[::-1])

# escape character \
# print("spark is "easy" to learn")
print("spark is \"easy\" to learn")

x = "spark is easy \n learning spark is exciting \n and useful"
print(x)

print(dir(str))

