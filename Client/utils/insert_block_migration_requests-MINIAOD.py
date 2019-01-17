#!/usr/bin/env python
from __future__ import print_function
from dbs.apis.dbsClient import DbsApi
"""
This is a test for migration server to migrate mutiple blocks at the same time and these blocks share 
parent or grandparent blocks.
The MINIAOD datasets usually have these blocks. The SQL to find them are

select block_name, block_id, dataset_id from  CMS_DBS3_PROD_GLOBAL_OWNER.blocks where dataset_id 
in (select  DATASET_ID from CMS_DBS3_PROD_GLOBAL_OWNER.DATASETS DS
    join  CMS_DBS3_PROD_GLOBAL_OWNER.DATASET_PARENTS P on P.THIS_DATASET_ID=DS.DATASET_ID
    where DATA_TIER_ID = 31223); 

"""

migration_url = "https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/"
input_url = "https://cmsweb-testbed.cern.ch:8443/dbs/int/phys03/DBSMigrate/"

api= DbsApi(url=input_url)
migration_input = [
'/SingleMu/CMSSW_7_2_0_pre3-GR_R_72_V2_frozenHLT_RelVal_mu2012D-v1/MINIAOD#a793521e-1db0-11e4-84b8-003048c9c3fe', 
'/SingleMu/CMSSW_7_2_0_pre3-GR_R_72_V2_frozenHLT_RelVal_mu2012D-v1/MINIAOD#16b84cb6-1e2f-11e4-84b8-003048c9c3fe',
'/SingleMu/CMSSW_7_2_0_pre3-GR_R_72_V2_frozenHLT_RelVal_mu2012D-v1/MINIAOD#d584a804-1d23-11e4-84b8-003048c9c3fe',
'/SingleMu/CMSSW_7_2_0_pre3-GR_R_72_V2_frozenHLT_RelVal_mu2012D-v1/MINIAOD#b185d2c6-1d68-11e4-84b8-003048c9c3fe'
]

for b in migration_input:
    b = b.strip()
    m  = dict(migration_url=migration_url, migration_input=b)
    print (m)
    try:
        print (api.submitMigration(m))
    except Exception as e:
	print (e)
        pass 
