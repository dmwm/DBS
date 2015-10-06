from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
import os

url=os.environ['DBS_WRITER_URL']
# API Object    
dbs3api = DbsApi(url=url)

for j in range(100, 105):
    acq_era={'acquisition_era_name': 'YuYi_GUO%s' %(j), 'description': 'testing_insert_era2',
         'start_date':1234567890}

    print(dbs3api.insertAcquisitionEra(acq_era))
    

print(dbs3api.listAcquisitionEras_ci(acquisition_era_name='YuYi_GUO%'))

print("case seneitive")

print(dbs3api.listAcquisitionEras(acquisition_era_name='YuYi_GUO%'))

#print dbs3api.updateAcqEraEndDate(acquisition_era_name='YUYI-GUO10', end_date=100)

