

def avg(file_name):
    f = open(file_name)
    num = 0
    sum_el = 0
    for line in f:
        if num == 91:
            break
        if num > 3:
            sum_el += float(line)
        num += 1
    num -= 3
    f.close()
    print round(sum_el / num, 2)

def avg2(file_name):
    f = open(file_name)
    num = 0
    sum_el1 = 0
    sum_el2 = 0
    for line in f:
        if num == 91:
            break
        vals = line.split()
        if num > 3:
            sum_el1 += float(vals[0])
            sum_el2 += float(vals[1])
        num += 1
    num -= 3
    f.close()
    print round(sum_el1 / num, 2), round(sum_el2 / num, 2), round((sum_el1 + sum_el2) / num, 2)

print 'CPU'
avg('cpu2.out')
avg('cpu12.out')
avg('cpu11.out')
avg('cpu8.out')

print 'MEMORY'
avg('mem2.out')
avg('mem12.out')
avg('mem11.out')
avg('mem8.out')

print 'DISK'
avg2('dsk2.out')
avg2('dsk12.out')
avg2('dsk11.out')
avg2('dsk8.out')

print 'NET'
avg2('net2.out')
avg2('net12.out')
avg2('net11.out')
avg2('net8.out')

print 'SD-CPU'
avg('sd-cpu2.out')
avg('sd-cpu12.out')
avg('sd-cpu11.out')
avg('sd-cpu8.out')

print 'SD-MEM'
avg('sd-mem2.out')
avg('sd-mem12.out')
avg('sd-mem11.out')
avg('sd-mem8.out')
