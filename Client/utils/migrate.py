from dbs.apis.dbsClient import *
src_url="http://localhost:8585"
dst_url="http://localhost:8586"
dbs3api = DbsApi(url=dst_url)

data = dict(migration_url=src_url, migration_dataset='/RelValbJpsiX/CMSSW_2_0_0-RelVal-1207932667/RAW')
print dbs3api.migrateStart(data)

#for i in xrange(50):
#    data = dict(migration_url=src_url, migration_dataset='data_03_%i' % i) 
#    print dbs3api.migrateStart(data)

