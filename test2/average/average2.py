import matplotlib.pyplot as plt

x1_vals = []
x2_vals = []
x3_vals = []
x4_vals = []
time_now = 0
time_vals = []
f = open('dsk2.out', 'r')
for line in f:
    val = line.split()
    x1_vals.append(float(val[0]) + float(val[1]))
    time_now += 10
    time_vals.append(time_now)
f.close()
f = open('dsk8.out', 'r')
for line in f:
    val = line.split()
    x2_vals.append(float(val[0]) + float(val[1]))
f.close()
f = open('dsk11.out', 'r')
for line in f:
    val = line.split()
    x3_vals.append(float(val[0]) + float(val[1]))
f.close()
f = open('dsk12.out', 'r')
for line in f:
    val = line.split()
    x4_vals.append(float(val[0]) + float(val[1]))
f.close()
#plt.plot(time_vals, x1_vals, 'r', time_vals, x2_vals, 'b', time_vals, x3_vals, 'g')
line1, = plt.plot(time_vals, x1_vals, 'r--', linewidth=2.0)
line1.set_label('1st strategy')
line2, = plt.plot(time_vals, x2_vals, 'b--', linewidth=2.0)
line2.set_label('3rd strategy')
line3, = plt.plot(time_vals, x3_vals, 'g--', linewidth=2.0)
line3.set_label('2nd strategy')
line4, = plt.plot(time_vals, x4_vals, 'm--', linewidth=2.0)
line4.set_label('4th strategy')
plt.axis([0, 975, 0, 70])
plt.xlabel("Time (Seconds)")
plt.ylabel("Disk Usage (MB/s)")
plt.legend((line1, line4, line3, line2), ('1st strategy', '2nd strategy', '3rd stragety', '4th strategy'), loc='upper right')
plt.show()
