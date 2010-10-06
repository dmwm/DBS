#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8687/DBS"
# API Object    
dbs3api = DbsApi(url=url)
blockdump={'dataset_conf_list': [{u'release_version': 'CMSSW_1_2_3', u'pset_hash':
'76e303993a1c2f842159dbfeeed9a0dd', u'app_name': 'cmsRun', u'output_module_label': 'Merged'}]
,'file_conf_list': [{u'release_version': 'CMSSW_1_2_30', u'pset_hash':
'76e303993a1c2f842159dbfeeed9a0dd', u'lfn': '/store/mc/1414479/2.root',
u'app_name': 'cmsRun', u'output_module_label': 'Merged'}],

'files': [{u'check_sum': '1504266448', 'file_lumi_list': [{u'lumi_section_num':
27414, u'run_num': 1}, {u'lumi_section_num': 26422, u'run_num': 2}], 
u'adler32':'NOTSET', u'event_count': 1619, u'file_type': 'EDM', u'create_by': 'Client Name', u'last_modified_by': 'Client Name', u'creation_date': 1279912089,
u'logical_file_name': '/store/mc/1414479/2.root', u'file_size': 2012211901,
u'last_modification_date': 1279912089, u'md5': None, u'auto_cross_section':
0.0, u'is_file_valid': 1}],

'block': {u'block_id': 18, u'create_by': 'Client Name', u'creation_date':
1279912079, u'open_for_writing': 1, u'block_name':
'/unittest_web_primary_ds_name_1414400/unittest_web_dataset_14144/GEN-SIM-RAW#1414400',
u'file_count': 10, u'origin_site_name': 'cmssrm.fnal.gov', u'dataset_id':
21, u'block_size': 20122119010},


'block_parent_list': [{u'block_name':
'/unittest_web_primary_ds_name_14144/parent_unittest_web_dataset_14144/GEN-SIM-RAW#14144'}],

'processing_era': {'create_by': 'Client Name', 'processing_version':
'4199', 'description': 'this is a test', 'creation_date': 1279912078},

'ds_parent_list': [{u'parent_dataset':
'/unittest_web_primary_ds_name_14144/parent_unittest_web_dataset_14144/GEN-SIM-RAW'}],
'acquisition_era': {'acquisition_era_name': 'acq_era_14144', 'create_by':
'Client Name', 'description': None, 'creation_date': 1279912078},

'primds':
{u'create_by': 'Client Name', u'primary_ds_type': 'TEST', u'primary_ds_id':
15, u'primary_ds_name': 'unittest_web_primary_ds_name_1414400',
u'creation_date': 1279912078},

'dataset': {u'is_dataset_valid': 1,
u'physics_group_name': 'Tracker', u'create_by': 'Client Name',
u'dataset_access_type': 'PRODUCTION', u'data_tier_name': 'GEN-SIM-RAW',
u'last_modified_by': 'Client Name', u'creation_date': 1279912078,
u'processed_ds_name': 'unittest_web_dataset_14144', u'global_tag': None,
u'xtcrosssection': 123.0, u'last_modification_date': 1279912078,
u'dataset_id': 21, u'dataset':
'/unittest_web_primary_ds_name_1414479/unittest_web_dataset_14144799/GEN-SIM-RAW'},

'file_parent_list':
[
{u'parent_logical_file_name': '/store/mc/parent_14144/2.root',
u'parent_file_id': 683, u'logical_file_name': '/store/mc/1414479/2.root'}
]}

try:
    dbs3api.insertBlockBluk(blockdump)
except Exception, e:
    print e

