a = 15
b = 20
c = 30
print(a+b+c)
print((a+b)-c)
print(a*b)
print(a/b)
print(a%b)

def add_funcs():
    res=a + b
    if res > 20:
        print(' a+b is greater than 20 - TRUE')
    elif res < 20:
        print('a+b is lesser greater than 20false')
    print('a is greater than b -', a>b)

    print('a is equal to b -',a==b)
    print('a is b -', a is b)
    print('address of a is -',id(a))
    print('address of b is -',id(b))

    kip=[a,b,c]
    pik=[a,b,c]
    print('kip - ',kip)
    print('pik -', pik)
    print(' kip is equal to pik - ',kip == pik)
    print('kip is pik - ',kip is pik)

x = 10

x = x+1  OR x += 1
x = x-1  OR x -= 1


add_funcs()