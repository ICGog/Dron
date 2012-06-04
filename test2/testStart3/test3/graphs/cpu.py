import matplotlib.pyplot as plt

f = open('dron-cpu.in', 'r')

vals = []
time_vals = []
time_now = 0
num_vals = 0
for line in f:
    time_vals.append(time_now)
    time_now += 5
    vals.append(int(line))
    num_vals += 1

f.close()

#plt.plot(time_vals, vals, 'b')
#plt.axis([0, 975, 0, 100])
#plt.xlabel("Time (Seconds)")
#plt.ylabel("CPU Usage (%)")
#plt.show()

f = open('dron-cpu.out', 'w')
for val in vals:
    f.write(str(val) + '\n')
f.close()
