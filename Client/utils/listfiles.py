#DBS-3 imports
import time
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
#url="http://vocms09.cern.ch:8586/dbs3"
url="http://vocms09.cern.ch:8585/dbs3"
#url="http://vocms09.cern.ch:8989/DBSServlet"
# API Object    
dbs3api = DbsApi(url=url)
t1=time.time()
#print dbs3api.listFile(dataset="/RelValProdTTbar/CMSSW_3_1_0_pre11-MC_31X_V1-v1/GEN-SIM-RAW")
print dbs3api.listFile(dataset="/QCD_Pt15/Summer09-MC_31X_V3-v1/GEN-SIM-RAW")
print url
t2=time.time()-t1
print t2

