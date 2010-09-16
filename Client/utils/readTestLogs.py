#! /usr/local/bin/python
import os
import sys

try:
    fw = open('testResultGT1500.txt', 'w')
except IOError:
    print 'Can\'t open file for writing.'
    sys.exit(0)

#fw.writelines("weight                         time\n")

path="log21-4"  # insert the path to the directory of interest
dirList=os.listdir(path)
for fname in dirList:
    fname="log21-4/"+fname 
    print fname
    try:
	fr = open(fname, 'r')
	for a in fr.readlines():
	    if "Time Spent" in a :
		#print a
		data = a.split(":", 3)
		#print data[1], ' + ', data[2]
		time = data[1].split()
		#print time[0]
		weight = data[2].split()
		l = ("%s      %s\n")  %(weight[0], time[0])
		#print l 
		if int(weight[0])>1500:
		    fw.write(l)
		    #hz = int(weight[0])/float(time[0])
		    #print hz
		    #fw.writelines(str(hz)+"\n")
	fr.close()
    except IOError:
	print 'Cannot open file %s for reading.' %fname
fw.close()
    
