import matplotlib.pyplot as plt

x1_vals = []
x2_vals = []
x3_vals = []
x4_vals = []
time_now = 0
time_vals = []
f = open('sd-cpu2.out', 'r') # 1
for line in f:
    x1_vals.append(float(line))
    time_vals.append(time_now)
    time_now += 25
f.close()
f = open('sd-cpu8.out', 'r') # 4
for line in f:
    x2_vals.append(float(line))
f.close()
f = open('sd-cpu11.out', 'r') # 3
for line in f:
    x3_vals.append(float(line))
f = open('sd-cpu12.out', 'r') # 2
for line in f:
    x4_vals.append(float(line))    
f.close()
#plt.plot(time_vals, x1_vals, 'r', time_vals, x2_vals, 'b', time_vals, x3_vals, 'g')
line1, = plt.plot(time_vals, x1_vals, 'r--', linewidth=2.0)
line1.set_label('1st strategy')
line2, = plt.plot(time_vals, x2_vals, 'b--', linewidth=2.0)
line2.set_label('4th strategy')
line3, = plt.plot(time_vals, x3_vals, 'g--', linewidth=2.0)
line3.set_label('3rd strategy')
line4, = plt.plot(time_vals, x4_vals, 'm--', linewidth=2.0)
line4.set_label('2nd strategy')
plt.axis([0, 950, 0, 70])
plt.xlabel("Time (Seconds)")
#plt.ylabel("Memory Usage (%)")
plt.ylabel("CPU Usage Standard Deviation (%)")
plt.legend((line1, line4, line3, line2), ('1st strategy', '2nd strategy', '3rd stragety', '4th strategy'), loc='upper right')
plt.show()
