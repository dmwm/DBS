#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)

print dbs3api.listDataTiers()

print dbs3api.listDataTiers(datatier='AOD')

