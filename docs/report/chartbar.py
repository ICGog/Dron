from pylab import *

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2., 0.85*height, '%d'%int(height),
                ha='center', va='bottom')

#axis([0.0, 1060.0, 0, 240])

#pos = array([20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 380, 400, 420, 440, 460, 480, 500, 520, 540, 560, 580, 600, 620, 640, 660, 680, 700, 720, 740, 760, 780, 800, 820, 840, 860, 880, 900, 920, 940, 960, 980, 1000, 1020, 1040])

#val = array([199,198,195,204,38,0,37, 0, 0, 0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 57, 0, 0, 0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0 ,0, 23])

#axis([0.5, 1.0, 0, 50])
#pos = array([0.6, 0.7, 0.8, 0.9])
#val = array([8, 42, 8, 42])

axis([0.0, 30.0, 0, 6])
pos = array([2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29])
val = array([1,3,3,4,5,3,4,4,4,4,3,5,3,5,4,3,5,4,4,4,3,3,5,5,3,3,2,1])

xlabel("Delay(sec)")
ylabel("Number Jobs")

#colors = array(['g', 'g', 'g', 'g', 'g', 'g'])

#xticks(pos, ('0.6', '0.7', '0.8', '0.9'))

rects = bar(pos,val, align='center', width=0.7, color='g')#, color=colors)
#autolabel(rects)
show()
