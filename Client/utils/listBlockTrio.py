
#DBS-3 imports
from dbs.apis.dbsClient import *
import time
#url="https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/"
url="https://dbs3-test2.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
block ="/QCD_Pt_600to800_TuneCP5_13TeV_pythia8/RunIISummer19UL18MiniAOD-106X_upgrade2018_realistic_v11_L1v1-v2/MINIAODSIM#ee83a871-c4a1-4d20-bd24-a599220e2459"
try:
    t1 = 0
    t2 = 0
    t = time.time()
    print(block)
    for i in dbs3api.listBlockTrio(block_name=block):
        #print(i)
        t1 += 1
        for k, v in list(i.items()):
            t2 += len(v)        
    print(t1)
    print(t2)
    print(time.time()-t)
except Exception as e:
    print (e)
