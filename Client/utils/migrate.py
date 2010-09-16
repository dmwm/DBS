from dbs.apis.dbsClient import *
src_url="http://localhost:8585"
dst_url="http://cmssrv18.fnal.gov:8585/MIGRATE"
dbs3api = DbsApi(url=dst_url)
"""
# Enqueue a dataset migration request
data = dict(migration_url=src_url, migration_input='/RelValbJpsiX/CMSSW_2_0_0-RelVal-1207932667/RAW')
print dbs3api.migrateSubmit(data)
# Enqueu a block migration request
data = dict(migration_url=src_url, migration_input='/RelValbJpsiX/CMSSW_2_0_0-RelVal-1207932667/RAW#1234_5678_910')
print dbs3api.migrateSubmit(data)
#   

data = dict(migration_url="http://vocms08.cern.ch:8585/DBS", migration_input="/Cosmics/CRAFT09-CRAFT09_R_V4_CosmicsSeq_v1/RECO#33f71127-6994-4c1c-82aa-3bf9c4de8f45")
print dbs3api.migrateSubmit(data)

data= dict(migration_url="http://vocms08.cern.ch:8585/DBS", migration_input="/Cosmics/CRAFT09-CRAFT09_R_V4_CosmicsSeq_v1/RECO#c218c8a5-8bc7-418f-9257-7993be8fb1d5")
print dbs3api.migrateSubmit(data)
"""

data = dict(migration_url="http://vocms08.cern.ch:8585/DBS", migration_input='/Cosmics/CRAFT09-CRAFT09_R_V4_CosmicsSeq_v1/RECO')
print dbs3api.migrateSubmit(data)
    
#for i in xrange(50):
#    data = dict(migration_url=src_url, migration_dataset='data_03_%i' % i) 
#    print dbs3api.migrateStart(data)

