import matplotlib.pyplot as plt
import math

vals = []

f = open('cpu-1.in', 'r')
time_vals = []
time_now = 0
num_vals = 0
for line in f:
    val = int(line)    
    vals.append(val)
    num_vals += 1
f.close()

for num in range(2,21):
    f = open('cpu-' + str(num) + '.in', 'r')
    print num
    cnt = 0
    for line in f:
        val = int(line)
        vals[cnt] += val
        cnt += 1
    f.close()

y_vals = []

f = open('cpu.out', 'w')
for num in range(0, num_vals / 2):
    val = 0
    time_now += 10
    time_vals.append(time_now)
    for num2 in range(0, 2):
        val += vals[num * 2 + num2]
    val /= 2.0
    f.write(str(val / 20.0) + '\n')
#    y_vals.append(val / 20.0)

#plt.plot(time_vals, y_vals)
#plt.axis([0, 975, 0, 100])
#plt.show()

f.close()

