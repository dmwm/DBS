#DBS-3 imports
import time
from dbs.apis.dbsClient import *
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
t1=time.time()
#print dbs3api.listFile(dataset="/RelValProdTTbar/CMSSW_3_1_0_pre11-MC_31X_V1-v1/GEN-SIM-RAW")
print dbs3api.listFiles(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW",validFileOnly=1 )
print "****************************************"
print dbs3api.listFiles(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW",validFileOnly=1, detail=1 )
print url
t2=time.time()-t1
print t2

