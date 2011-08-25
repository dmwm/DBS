#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url for tunneling through lxplus 
url="https://localhost:1443/dbs/DBSWriter"
# API Object    
dbs3api = DbsApi(url=url)
datatier = {'data_tier_name' : 'GEN-SIM-DBSDEBUG2' }
try:
    dbs3api.insertDataTier(datatier)
    print "All Done"
except Exception, e:
    print e

