import matplotlib.pyplot as plt

f = open('dron-mem.in', 'r')

mem_total = 7885012.0
vals = []
time_vals = []
time_now = 0
num_vals = 0
for line in f:
    time_vals.append(time_now)
    time_now += 5
    val = line.split()
    vals.append(round((mem_total - float(val[0]) - float(val[1]) - float(val[2])) / mem_total * 100.0, 2))
    num_vals += 1

f.close()

#plt.plot(time_vals, vals, 'b')
#plt.axis([0, 975, 0, 100])
#plt.xlabel("Time (Seconds)")
#plt.ylabel("Memory Usage (%)")
#plt.show()

f = open('dron-mem.out', 'w')
for val in vals:
    f.write(str(round(val, 2)) + '\n')
f.close()
