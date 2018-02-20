"""
client writer unittests
"""
from __future__ import print_function
import os, sys, imp
import time
import uuid
import unittest
from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from ctypes import *
import json

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

class DBSClientBlockWriter_t(unittest.TestCase):
    testparams = None
    uid = None

    def __init__(self, methodName='runTest'):
        super(DBSClientBlockWriter_t, self).__init__(methodName)
        if not (self.testparams and self.uid):
            self.setUpClass()
        url=os.environ['DBS_WRITER_URL']
        proxy=os.environ.get('SOCKS5_PROXY')
        self.api = DbsApi(url=url, proxy=proxy)

    @classmethod
    def setUpClass(cls):
        """Class method to set-up the class"""
        ### necessary since one instance per test case is created and pid and testparams need to be shared between instances
        infofile=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blockdump.dict'), "r")
        cls.testparams=importCode(infofile, "testparams", 0).blockDump
        cls.uid = uuid.uuid4().time_mid
        print("****uid=%s******" % cls.uid)

    def setUp(self):
        """setup all necessary parameters"""
        pass

    def test100(self):
        """test100 Negitive test: insert block with missing check_sum, adler32 or md5. """
        self.assertRaises(HTTPError, self.api.insertBulkBlock, blockDump=self.testparams)

    def test200(self):
        """test200 Negitive test: insert block with data tier not in DBS. """
        self.testparams['dataset']['data_tier_name'] =  'YUYI_TEST' 
        self.assertRaises(HTTPError, self.api.insertBulkBlock, blockDump=self.testparams)



    def test1000(self):
        """test1000 web.DBSClientWriter.insertBlockBulk: basic test"""
        self.testparams['dataset_conf_list'][0]['app_name'] = "%s_%s"%(self.testparams['dataset_conf_list'][0]['app_name'], self.uid)
        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['app_name'] = "%s_%s"%(self.testparams['file_conf_list'][i]['app_name'], self.uid)
            self.testparams['file_conf_list'][i]['lfn'] = self.testparams['file_conf_list'][i]['lfn'].replace('.root', '_%s.root' %(self.uid))
        ct = 1 
        for k in range(len(self.testparams['files'])):
             for l in self.testparams['files'][k]['file_lumi_list']:
                 ct +=1
                 l['event_count'] = ct    
             self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root', '_%s.root' % (self.uid))
             self.testparams['files'][k]['adler32'] = '123abc'

        self.testparams['primds']['primary_ds_name'] ='%s_%s' %(self.testparams['primds']['primary_ds_name'], self.uid)

        self.testparams['dataset']['dataset'] = (self.testparams['dataset']['dataset']).replace("14144", str(self.uid))

        self.testparams['block']['block_name'] = self.testparams['block']['block_name'].replace("14144", str(self.uid))
        #We hard coded the parent_logical_fil_name in the dict file for testing on lum db. It may not
        #fit to ask dbs. One have to change it before run the test for other dbs.
        #for  k in range(len(self.testparams['file_parent_list'])):
        #    self.testparams['file_parent_list'][k]['logical_file_name'] = "%s_%s" %(self.testparams['file_parent_list'][k]['logical_file_name'],self.uid)
        self.api.insertBulkBlock(blockDump=self.testparams)
        print("\nDone inserting parent block with events per lumi: ", self.testparams['block']['block_name'])

    def test1001(self):
        """insert chidren with parentage: the privious inserted files are the parents"""
        self.testparams['file_parent_list'] = []
        for k in range(len(self.testparams['files'])):
            self.testparams['file_parent_list'].append({'logical_file_name': self.testparams['files'][k]['logical_file_name'].replace('.root', '_child.root'),
                                             'parent_logical_file_name': self.testparams['files'][k]['logical_file_name']})
            self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root', '_child.root')
        self.testparams['dataset']['dataset'] = '%s-%s' %(self.testparams['dataset']['dataset'], 'CHD')
	#print self.testparams['dataset']['dataset']
        self.testparams['block']['block_name'] = self.testparams['block']['block_name'].replace("#", "#00")
	#print self.testparams['block']['block_name']
        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['lfn'] =  self.testparams['file_conf_list'][i]['lfn'].replace('.root', '_child.root')
        self.api.insertBulkBlock(blockDump=self.testparams)
        print("\nDone inserting child blocks:  ", self.testparams['block']['block_name'])

    def test1002(self):
        """insert duplicated block"""
        self.assertRaises(HTTPError, self.api.insertBulkBlock, blockDump=self.testparams)
    
    def test2000(self):
        """test2000 web.DBSClientWriter.insertBlockBulk with mixed event_count/lumi: basic test\n"""
        
        infofile=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blockdump.dict'), "r")
        self.testparams=importCode(infofile, "testparams", 0).blockDump
        self.uid = uuid.uuid4().time_mid

        self.testparams['dataset_conf_list'][0]['app_name'] = "%s_%s"%(self.testparams['dataset_conf_list'][0]['app_name'], self.uid+10)
        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['app_name'] = "%s_%s"%(self.testparams['file_conf_list'][i]['app_name'], self.uid+10)
            self.testparams['file_conf_list'][i]['lfn'] = self.testparams['file_conf_list'][i]['lfn'].replace('.root', '_%s.root' %(self.uid+10))
        ct = 1
        for k in range(len(self.testparams['files'])):
             
             for l in self.testparams['files'][k]['file_lumi_list']:
                 ct +=1
                 if ct%2 == 0:
                     l['event_count'] = ct
             self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root', '_%s.root' % (self.uid+10))
             self.testparams['files'][k]['adler32'] = '123abc'

        self.testparams['primds']['primary_ds_name'] ='%s_%s' %(self.testparams['primds']['primary_ds_name'], self.uid+10)

        self.testparams['dataset']['dataset'] = (self.testparams['dataset']['dataset']).replace('14144', str(self.uid+10))

        self.testparams['block']['block_name'] = self.testparams['block']['block_name'].replace('14144', str(self.uid+10))
        print("\ninserting block with mixed events per lumi: ", self.testparams['block']['block_name'])
        self.assertRaises(dbsClientException, self.api.insertBulkBlock, blockDump=self.testparams)

    def test3000(self):
        """test3000 web.DBSClientWriter.insertBlockBulk without event per lumi: basic test\n"""

        infofile=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'blockdump.dict'), "r")
        self.testparams=importCode(infofile, "testparams", 0).blockDump
        self.uid = uuid.uuid4().time_mid

        self.testparams['dataset_conf_list'][0]['app_name'] = "%s_%s"%(self.testparams['dataset_conf_list'][0]['app_name'], self.uid+10)
        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['app_name'] = "%s_%s"%(self.testparams['file_conf_list'][i]['app_name'], self.uid+10)
            self.testparams['file_conf_list'][i]['lfn'] = self.testparams['file_conf_list'][i]['lfn'].replace('.root', '_%s.root' %(self.uid+10))
        ct = 1
        for k in range(len(self.testparams['files'])):
             self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root', '_%s.root' % (self.uid+10))
             self.testparams['files'][k]['adler32'] = '123abc'
        self.testparams['primds']['primary_ds_name'] ='%s_%s' %(self.testparams['primds']['primary_ds_name'], self.uid+10)

        self.testparams['dataset']['dataset'] = (self.testparams['dataset']['dataset']).replace('14144', str(self.uid+10))

        self.testparams['block']['block_name'] = self.testparams['block']['block_name'].replace('14144', str(self.uid+10))
        print("\ninserting block without events per lumi: ", self.testparams['block']['block_name'])
        self.api.insertBulkBlock(blockDump=self.testparams)



if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientBlockWriter_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
