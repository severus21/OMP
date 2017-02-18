from random import randint, random, seed
from math import *

def wrand( n, x0):
    a = 6364136223846793005;
    b = 1; 
    m = min( n+1, 2**64)                
    return (a * ((a * x0 + b) % m) + b) % m 
n=2

buckets = list(range(0,n))
load = list([1/(2*(n-1)) for i in range(0,n)])
load[0] = 1/2
print(load)
M = sum(load)
load = [x/M for x in load]

print( "load sum",sum(load) )

stats = [ 0 for b in buckets]

#calcul log
A = [0 for i in buckets]
for i in buckets:
    a = 0 
    for j in range(n):
        if j != i:
            a -= log(load[j])
    A[i] = a   
    print(i, a)
I = 10
for x in range(I):
    tmp = [ log(wrand(b,x)+1)  for b in buckets]#car le log est croissant
    stats[ tmp.index( max(tmp))]+= 1
m= max( stats)
stats =  [ s/m for s in stats]

print(stats)    
