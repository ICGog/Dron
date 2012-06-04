import matplotlib.pyplot as plt
import math

vals = []

mem_total = 7885012.0
f = open('mem-1.in', 'r')
time_vals = []
time_now = 0
num_vals = 0
for line in f:
    val = line.split()
    vals.append(round((mem_total - float(val[0]) - float(val[1]) - float(val[2])) / mem_total * 100.0, 2))
    num_vals += 1
f.close()

for num in range(2,21):
    f = open('mem-' + str(num) + '.in', 'r')
    print num
    cnt = 0
    for line in f:
        val = line.split()
        vals[cnt] += round((mem_total - float(val[0]) - float(val[1]) - float(val[2])) / mem_total * 100.0, 2)
        cnt += 1
    f.close()

y_vals = []

f = open('mem.out', 'w')
for num in range(0, num_vals / 2):
    val = 0
    time_now += 10
    time_vals.append(time_now)
    for num2 in range(0, 2):
        val += vals[num * 2 + num2]
    val /= 2.0
    f.write(str(round(val / 20.0, 2)) + '\n')
