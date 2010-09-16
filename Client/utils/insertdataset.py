#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8989/dbs3"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)

dataset={'is_dataset_valid': 1, 'primary_ds_name': 'anzar22', 'physics_group_name': 'Tracker',
'global_tag': 'startup31x_v3::all', 'primary_ds_type': 'test', 'processed_ds_name':
'Summer09-STARTUP31X_V3-v1', 'dataset': '/anzar22/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW' , 'dataset_type': 'PRODUCTION', 'xtcrosssection': 123, 'data_tier_name': 'GEN-SIM-DIGI-RAW'}

print dataset.keys()

print dbs3api.insertDataset(dataset)
# Is service Alive
"""
print dbs3api.ping()
print dbs3api.listPrimaryDataset("Wmunu_Wplus-powheg")
print dbs3api.listPrimaryDataset("ANZAR003")
print dbs3api.listPrimaryDataset("ANZAR004")
"""
