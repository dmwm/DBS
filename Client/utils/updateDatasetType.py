
#DBS-3 imports
from dbs.apis.dbsClient import *
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSWriter"
# API Object    
dbs3api = DbsApi(url=url)

dataset='/unittest_web_primary_ds_name_1492472201/Acq_Era_10018-unittest_web_dataset-v19/GEN-SIM-RAW'

print(dbs3api.listDatasets(dataset=dataset, detail=1, dataset_access_type='*'))

status = 'VALID' 
dbs3api.updateDatasetType(dataset=dataset, dataset_access_type= status)

print(dbs3api.listDatasets(dataset=dataset, detail=1, dataset_access_type='*'))
