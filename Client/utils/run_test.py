import subprocess
#
#subprocess.call("python2.6 test.py", shell=True)
#subprocess.call("ls -1", shell=True)
#
url="http://vocms09.cern.ch:8989/DBSServlet"
ds_fl=open("test_datasets.txt", "r")
ds_lst=ds_fl.readlines()
ds_fl.close()
procs={}
count=0
done_ds=""

#import pdb
#pdb.set_trace()

for adst in ds_lst:
	if count == 10 : break
	if adst.startswith("d ") : continue
	else : 
		cmd="python2.6 dbs2Todbs3DatasetMigrate.py %s %s" % (url, adst)
		print cmd
		procs[adst]=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
	count += 1
	done_ds += "d "+adst+"\n"

"""
wait = 1
while wait == 1:
	wait = 0
	for item in procs.keys():
		if procs[item].poll() == None:
			wait = 1
"""
		
for item in procs.keys():
	stdout_value = procs[item].communicate()[0]
	print stdout_value

doneds_fs=open("done_datasets.txt", "w")
doneds_fs.write(done_ds)
doneds_fs.close()
	
