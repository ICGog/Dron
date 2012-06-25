from pylab import *

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2., 0.85*height, '%d'%int(height),
                ha='center', va='bottom')

axis([0.0, 1.75, 0, 1200])

pos = array([0.5, 0.75, 1, 1.25])

val = array([904,897,876,938])

xlabel("Strategy")
ylabel("Duration(sec)")

colors = array(['r', 'm', 'g', 'b'])

xticks(pos, ('1st', '2nd', '3rd', '4th'))

rects = bar(pos,val, align='center', width=0.2, color=colors)
autolabel(rects)
show()
