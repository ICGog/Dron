import matplotlib.pyplot as plt
import math

sq_vals = []
vals = []

f = open('cpu-1.in', 'r')
time_vals = []
time_now = 0
num_vals = 0
for line in f:
    val = int(line)    
    sq_vals.append(val * val)
    vals.append(val)
    num_vals += 1
f.close()

for num in range(2,21):
    f = open('cpu-' + str(num) + '.in', 'r')
    print num
    cnt = 0
    for line in f:
        val = int(line)
        sq_vals[cnt] += val * val
        vals[cnt] += val
        cnt += 1
    f.close()

y_vals = []

for num in range(0, num_vals / 5):
    sq_val = 0
    val = 0
    time_now += 25
    time_vals.append(time_now)
    for num2 in range(0, 5):
        sq_val += sq_vals[num * 5 + num2]
        val += vals[num * 5 + num2]
    sq_val /= 5.0
    val /= 5.0
    y_vals.append(math.sqrt(sq_val / 20.0 - (val / 20.0) * (val / 20.0)))

#plt.plot(time_vals, y_vals)
#plt.axis([0, 975, 0, 100])
#plt.show()

f = open('sd-cpu3.out', 'w')
for val in y_vals:
    f.write(str(round(val, 2)) + '\n')
f.close()
