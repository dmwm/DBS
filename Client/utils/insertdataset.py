from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
import os

url=os.environ['DBS_WRITER_URL']
# API Object    
dbs3api = DbsApi(url=url)

dataset={'primary_ds_name': 'yuyi_pri', 'physics_group_name': 'Tracker',
 'processed_ds_name':'YuYi_GUO2-pstr-v101', 'dataset_access_type': 'VALID',
 'xtcrosssection': 123, 'data_tier_name':
 'GEN-SIM-DIGI-RAW','acquisition_era_name':'YuYi_GUO2', 'processing_version':101 }

dataset.update({'dataset' : '/%s/%s/%s' %(dataset['primary_ds_name'], dataset['processed_ds_name'],
                dataset['data_tier_name'])})

print(dataset.keys())

print(dbs3api.insertDataset(dataset))
# Is service Alive
"""
print dbs3api.ping()
print dbs3api.listPrimaryDataset("Wmunu_Wplus-powheg")
print dbs3api.listPrimaryDataset("ANZAR003")
print dbs3api.listPrimaryDataset("ANZAR004")
"""
