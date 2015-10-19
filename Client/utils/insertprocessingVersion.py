from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
import os

url=os.environ['DBS_WRITER_URL']
# API Object    
dbs3api = DbsApi(url=url)

processing_version={'processing_version': 101, 'description': 'testing_insert_p-v'}

print(dbs3api.insertProcessingEra(processing_version))
print(dbs3api.listProcessingEras(processing_version=101))
