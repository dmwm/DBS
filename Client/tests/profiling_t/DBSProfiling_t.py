"""
DBS 3 Profiling Tests to check performance of the input validation
"""
import json
import os, time
import unittest
from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException

def generateUniqInput(input_file):
    f = file(input_file)
    input_json = json.load(f.read())
    f.close()
    
    unixtime = str(time.time())

    for entry in input_json['file_conf_list']:
        entry['lfn'] = "/store/user/test/%s%s" % (entry['lfn'], unixtime)

    data_tier_name = input_json['dataset']['data_tier_name']
    primary_ds = input_json['dataset']['dataset'].split("/")[1]
    processed_ds_name = input_json['dataset']['processed_ds_name']+unixtime

    input_json['dataset']['dataset'] = "/%s/%s/%s" % (primary_ds, processed_ds_name, data_tier_name)
    input_json['dataset']['processed_ds_name'] = processed_ds_name

    block = input_json['block']['block_name'].split("#")[1]
    input_json['block']['block_name'] = "/%s/%s/%s#%s" % (primary_ds, processed_ds_name, data_tier_name, block)

    for entry in input_json['files']:
        entry['logical_file_name'] = "/store/user/test/%s%s" % (entry['logical_file_name'], unixtime)
        
    return input_json

class DBSProfilingTests(unittest.TestCase):
    def setUp(self):
        url = os.environ.get('DBS_WRITER_URL', 'http://vocms08.cern.ch:8989/dbs/DBSWriter')
        self.api = DbsApi(url=url)
    
    def test_insert_block(self):
        input_json = generateUniqInput('/uscms/home/yuyi/dbs3-test/DBS/Client/utils/blockdump-f10-L10.dict')
        self.api.insertBlockBulk(input_json)

        #input_json = generateUniqInput('/afs/cern.ch/user/g/giffels/public/block-data-sample/blockdump-f100-L100.json')
        #self.api.insertBlockBulk(input_json)
        
        #input_json = generateUniqInput('/afs/cern.ch/user/g/giffels/public/block-data-sample/blockdump-f500-L500.json')
        #self.api.insertBlockBulk(input_json)

        #input_json = generateUniqInput('/afs/cern.ch/user/g/giffels/public/block-data-sample/blockdump-f1000-L1000.json')
        #self.api.insertBlockBulk(input_json)
    
    #def test_insert_datatier(self):
    #    input_dict = {'data_tier_name':'Super-Duper-Data-Tier_02'}
    #    self.api.insertDataTier(input_dict)
    
    #def test_list_datatier(self):
    #    self.api.listDataTiers(data_tier_name="GEN-SIM-DIGI-HLTDEBUG-RECO")
    
if __name__ == "__main__":
    
    TestSuite = unittest.TestSuite()

    loadedTests = unittest.TestLoader().loadTestsFromTestCase(DBSProfilingTests)
     
    TestSuite.addTests(loadedTests)
        
    unittest.TextTestRunner(verbosity=2).run(TestSuite)
