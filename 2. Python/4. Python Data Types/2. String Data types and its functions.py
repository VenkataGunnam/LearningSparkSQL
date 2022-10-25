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
z='venkat is here'
print(z.capitalize())
print(str.capitalize(z))
# print(dir(str))
print(str.lower(z))
print(str.upper(z))
print(str.center(z,20))
print(z.zfill(20))
print(str.count(z,'e'))
print(z.count('e'))
# in sql we call as trim , in python we call as strip
print(str.strip('    kdisl   '))
print(str.strip('00000STRIPED00000','0'))
print(str.find(z,'e'))
print(str.startswith(z,'v'))
print(str.endswith(z,'df'))
print(str.isdecimal('10'))

x = 'abc '.join(z)
print(x)

name = 'Hey guyz this is python programming from tutorial'
print(str.split(name,' '))
print(name.replace('tutorial', 'Tech Tutorial'))
