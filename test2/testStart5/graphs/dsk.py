import matplotlib.pyplot as plt

f = open('dron-1.in', 'r')

vals = []
time_vals = []
time_now = 0
for line in f:
    time_vals.append(time_now)
    time_now += 5
    val = line.split()
    vals.append((float(val[0]) + float(val[1]))/1024.0)

plt.plot(time_vals, vals)
plt.axis([0, 975, 0, 100])
plt.xlabel("Time (Seconds)")
plt.ylabel("Disk Usage (MB/s)")
plt.show()

f.close()

