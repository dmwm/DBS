#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
datatier = {'data_tier_name' : 'GEN-SIM-DBSDEBUG' }
try:
    dbs3api.insertDataTier(datatier)
except Exception, e:
    print e

