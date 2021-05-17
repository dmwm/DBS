
#DBS-3 imports
from dbs.apis.dbsClient import *
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader"
# API Object    
dbs3api = DbsApi(url=url)

dataset='/unittest_web_primary_ds_name_1492472201/Acq_Era_10018-unittest_web_dataset-v19/GEN-SIM-RAW'

print(dbs3api.listBlockSummaries(dataset=dataset))

