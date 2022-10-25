# tuples are assigned in the brackets
t = (1,2,3,4,5)
# tuple is immutable, this cannot be changed like list
# other than that all things works as list only
t1 = (1,2,3,4,5,6)
print(t1)
print(t1[3])

t2 = (9,8,7,6,5,5)
t3 = t2 +t1
print(t3)
# the below cannot be assigned
### t[3] = 5 -- error
# once created cannot be changed or inserted again

# set are assigned in flower brackets
# sets dont have index
# sets wont allow duplicate values
# set is also immutable, but here you can add and remove elements

s1 = {1,2,3,4,5,6}
s1.add(8)
print(s1)
s1.remove(4)
print(s1)
s2 = { 4,5,6,7,8 }
s3 = s1 - s2
print(s3)
s4 = s1 | s2
print(s4)
s5 = s1 & s2
print(s5)

y = { x for x in s5 if x %2 ==0 }
print(y)

z = { xk if xk %2 != 0 else 1 for xk in s5}
print(z)

print(s1)
print(s2)
fa1 = s1.difference(s2)
print(fa1)
s1.difference_update(s2)
print(s1,s2)
s2.difference_update(s1)
print(s1,s2)
s1.discard(1)
print(s1,s2)


## Dictinoary
## key value pair
## changeable
flowers = {'rose':'red',
           'lotus':'white',
           'lilly':'green'}
print(flowers)
print(flowers['rose'])
flowers['rose'] = 'Yellow'
print(flowers)
print(flowers.keys())
print(flowers.values())
print(flowers.items())
for x in flowers:
    print(x , flowers[x])

for x,y in flowers.items():
    print(x,'-',y)

a = (1 ,2 ,3, 4)
b = ('a')
c = dict.fromkeys(a,b)
print(c)




