#!/usr/bin/env python
#DBS-3 imports

from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url="http://vocms09.cern.ch:8989/DBSServlet"
#url="http://vocms09.cern.ch:8585/dbs3"
# API Object  
url=os.environ['DBS_WRITER_URL']
dbs3api = DbsApi(url=url)
#print dbs3api.listPrimaryDatasets()
try:
    dbs3api.insertPrimaryDataset({"primary_ds_type": "test", "primary_ds_name": "yuyi_pri"})
except Exception as e:
    print(e)
# Is service Alive
"""
print dbs3api.ping()
print dbs3api.listPrimaryDataset("Wmunu_Wplus-powheg")
print dbs3api.listPrimaryDataset("ANZAR003")
print dbs3api.listPrimaryDataset("ANZAR004")
"""
