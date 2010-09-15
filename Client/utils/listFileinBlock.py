import sys
import time

#DBS-3 imports
from dbs.apis.dbsClient import *
try:
        # Python 2.6
        import json
except:
        # Prior to 2.6 requires simplejson
        import simplejson as json

#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
#url="http://cmssrv18.fnal.gov:8585/dbs3"
url="http://vocms09.cern.ch:8989/DBSServlet"
if len(sys.argv) < 1:
    print "Usage: python %s <block>" %sys.argv[0]
    sys.exit(1)
		
# API Object    
dbs3api = DbsApi(url=url)
thisBlock = sys.argv[1]
#print thisBlock
# Is service Alive
#print dbs3api.ping()
#blocks= dbs3api.listBlock("/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#5ca8fd67-4bc1-4d24-a88c-f2dbf864793b")
start_time=time.time()
files = dbs3api.listFile(block=thisBlock)
end_time=time.time()
used_time = end_time - start_time 
#
json_f = json.loads(files)
list_f = json_f['result']
time_count={}
time_count["block"] = thisBlock
time_count["number_of_files"] =  len(list_f)
time_count["retrive_time"] = end_time - start_time
print time_count
