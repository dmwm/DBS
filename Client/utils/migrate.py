
#!/usr/bin/env python
from dbs.apis.dbsClient import DbsApi

src_url="https://cmsweb-testbed.cern.ch:8443/dbs/int/global/DBSReader"
dst_url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSMigrate"
api = DbsApi(url=dst_url)

data = dict(migration_url=src_url, migration_input='/AlCaPhiSymHcal/HAPPYHAPPYWARMFUZZY_T0TEST_WITHBUNNIESDANCINGAROUND-PFTHPFTHPTHFPFTHPHTH-v3EW35/RAW#04d0b5f4-26fa-4336-a8db-4b2295e20e6f')

try:
        print (api.submitMigration(data))
except Exception as e:
        print (e)
