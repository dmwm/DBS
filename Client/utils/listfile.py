#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8989/DBSServlet"
# API Object    
dbs3api = DbsApi(url=url)
# Is service Alive
print dbs3api.ping()
print dbs3api.listFile("/store/mc/Summer09/TTbar/GEN-SIM-RAW/MC_31X_V3-v1/0025/F4A93663-6988-DE11-8450-003048C559C4.root")
print dbs3api.listFile("sfile_300558.root")
