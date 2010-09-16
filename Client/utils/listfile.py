#DBS-3 imports
import time
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
# Is service Alive
#print dbs3api.ping()
#print dbs3api.listFile("sfile_300558.root")

# print dbs3api.listFile(lfn="/store/mc/Summer09/TTbar/GEN-SIM-RAW/MC_31X_V3-v1/0025/F4A93663-6988-DE11-8450-003048C559C4.root")
#print dbs3api.listFile(dataset="/TkCosmics38T/Summer09-STARTUP31X_V3_SuperPointing-v1/RAW-RECO")
#print dbs3api.listFile(dataset="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW")
t1=time.time()
dbs3api.listFile(block="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW#f99b4c09-a68f-4e73-8f4c-560c1fa922fc")
print url
t2=time.time()-t1
print t2


url="http://cmssrv48.fnal.gov:8989/DBSServlet"
api = DbsApi(url=url)
t1=time.time()
dbs3api.listFile(block="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW#f99b4c09-a68f-4e73-8f4c-560c1fa922fc")
print url
t2=time.time()-t1
print t2


