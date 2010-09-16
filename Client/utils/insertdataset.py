#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)

dataset={'IS_DATASET_VALID': 1, 'PRIMARY_DS_NAME': 'TkCosmics38T', 'PHYSICS_GROUP_NAME': 'Tracker', 'ACQUISITION_ERA_NAME': '', 'GLOBAL_TAG': 'STARTUP31X_V3::All', 'PRIMARY_DS_TYPE': 'test', 'PROCESSED_DATASET_NAME': 'Summer09-STARTUP31X_V3-v1', 'DATASET': '/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW', 'DATASET_TYPE': 'PRODUCTION', 'XTCROSSSECTION': 123, 'PROCESSING_VERSION': '1', 'DATA_TIER_NAME': 'GEN-SIM-DIGI-RAW'}

print dataset.keys()

print dbs3api.insertDataset(dataset)
# Is service Alive
"""
print dbs3api.ping()
print dbs3api.listPrimaryDataset("Wmunu_Wplus-powheg")
print dbs3api.listPrimaryDataset("ANZAR003")
print dbs3api.listPrimaryDataset("ANZAR004")
"""
