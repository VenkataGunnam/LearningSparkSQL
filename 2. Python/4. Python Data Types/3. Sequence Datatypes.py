lst = [1,2,3,4,5,6,7,8,9]
print(lst)
print(lst[0])
print(lst[7])
print(lst[0::8])
print(lst[::-1])
lst2 = list((1,2,3))
print(lst2)
lst3 = list('abcdef')
print(lst3)
del lst3
lst3 = list((4,5,6))
print(lst3)
lst4 = lst2 + lst3
print(lst4)
lst5 = lst2*4
print(lst5)

for i in lst4:
    print(i)

lst6 = [x for x in lst2]
lst7 = [x+10 for x in lst2]
print(lst7)
lst8 = [x*10 for x in lst2]
lst9 = [x  for x in lst2 if x>2]
print(lst9)
lst9 = [x  for x in lst2 if x>=2 ]
print(lst9)

pst = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
print(pst)

pst1 = [y.upper() for y in pst]
print(pst1)

pst2 = [y if y!='Thursday'  else 'Guruvar' for y in pst]
print(pst2)

pst3 = [y if 'e' in y else 'Holiday' for y in pst]
print(pst3)

## functions

print(min(lst3))
print(max(lst2))

flen = [12,5,8,9,6,3,1,4,7]
print(flen)
flen.append(8)
print(flen)
flen.remove(7)
print(flen)
flen.clear()
print(flen)
del flen
flen = [12,5,8,9,6,3,1,4,7]
print(flen)
flen2  = flen.copy()
print(flen2)
flen3 = flen
print(flen3)
flen3 = flen2[0::6]
print(flen3)
flen2 = flen2.pop()
print(flen2)
flen2 = [12,5,8,9,6,3,1,4,7]
print(flen2)
flen2.sort()
print(flen2)
print(sorted(flen3))




