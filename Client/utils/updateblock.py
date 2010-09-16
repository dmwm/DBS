#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block_name="/GlobalAug07-C/Online/RAW#ffe2395a-9c7c-434f-aad2-9b212f475df9"
open_for_writing=1
dbs3api.updateBlockStatus(block_name, open_for_writing)

