import matplotlib.pyplot as plt
import math

vals = []

f = open('dsk-1.in', 'r')
time_vals = []
time_now = 0
num_vals = 0
read_vals = []
write_vals = []
for line in f:
    val = line.split()
    read_vals.append(float(val[0]))
    write_vals.append(float(val[1]))
    num_vals += 1
f.close()

for num in range(2,21):
    f = open('dsk-' + str(num) + '.in', 'r')
    print num
    cnt = 0
    for line in f:
        val = line.split()
        read_vals[cnt] += float(val[0])
        write_vals[cnt] += float(val[1])
        cnt += 1
    f.close()

y_vals = []

f = open('dsk.out', 'w')
for num in range(0, num_vals / 2):
    read_val = 0
    write_val = 0
    time_now += 10
    time_vals.append(time_now)
    for num2 in range(0, 2):
        read_val += read_vals[num * 2 + num2]
        write_val += write_vals[num * 2 + num2]
    read_val /= 2.0
    write_val /= 2.0
    f.write(str(round(read_val / 20.0 / 1024.0, 2)) + ' ' + str(round(write_val / 20.0/ 1024, 2)) + '\n')
