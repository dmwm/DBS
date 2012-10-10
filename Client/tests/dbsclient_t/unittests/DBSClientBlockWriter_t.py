"""
client writer unittests
"""
import os, sys, imp
import time
import uuid
import unittest
from dbs.apis.dbsClient import *
from ctypes import *
import json

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

class DBSClientBlockWriter_t(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(DBSClientBlockWriter_t, self).__init__(methodName)
        self.setUpClass()
        url=os.environ['DBS_WRITER_URL']
        print url
        #proxy="socks5://localhost:5678"
        proxy=os.environ['SOCKS5_PROXY']
        print proxy
        self.api = DbsApi(url=url, proxy=proxy)

    @classmethod
    def setUpClass(cls):
        """Class method to set-up the class"""
        ### necessary since one instance per test case is created and pid and testparams need to be shared between instances
        infofile=open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'blockdump.dict'),"r")
        cls.testparams=importCode(infofile, "testparams", 0).blockDump
        cls.uid = uuid.uuid4().time_mid
        print "****uid=%s******" % cls.uid
               
    def setUp(self):
        """setup all necessary parameters"""
      	
    def test1000(self):
	"""test1000 web.DBSClientWriter.insertBlockBulk: basic test"""
        self.testparams['dataset_conf_list'][0]['app_name'] = "%s_%s"%(self.testparams['dataset_conf_list'][0]['app_name'], self.uid)
        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['app_name'] = "%s_%s"%(self.testparams['file_conf_list'][i]['app_name'], self.uid)
            self.testparams['file_conf_list'][i]['lfn'] = self.testparams['file_conf_list'][i]['lfn'].replace('.root','_%s.root' %(self.uid))

        for k in range(len(self.testparams['files'])):
             self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root', '_%s.root' % (self.uid))
             
        self.testparams['primds']['primary_ds_name'] ='%s_%s' %(self.testparams['primds']['primary_ds_name'], self.uid)

        self.testparams['dataset']['dataset'] = '%s_%s' %(self.testparams['dataset']['dataset'],self.uid)

        self.testparams['block']['block_name'] = '%s_%s' %(self.testparams['block']['block_name'],self.uid)
        
        #We hard coded the parent_logical_fil_name in the dict file for testing on lum db. It may not
        #fit to ask dbs. One have to change it before run the test for other dbs.
        #for  k in range(len(self.testparams['file_parent_list'])):
        #    self.testparams['file_parent_list'][k]['logical_file_name'] = "%s_%s" %(self.testparams['file_parent_list'][k]['logical_file_name'],self.uid)

	self.api.insertBulkBlock(blockDump=self.testparams)
        print "Done inserting parent files"
        
    def test1001(self):
	# insert chidren with parentage: the privious inserted files are the parents
        self.testparams['file_parent_list'] = []
        for k in range(len(self.testparams['files'])):
            self.testparams['file_parent_list'].append({'logical_file_name': self.testparams['files'][k]['logical_file_name'].replace('.root','_child.root'), 
                                             'parent_logical_file_name': self.testparams['files'][k]['logical_file_name']})
            self.testparams['files'][k]['logical_file_name'] = self.testparams['files'][k]['logical_file_name'].replace('.root','_child.root') 
        self.testparams['dataset']['dataset'] = '%s_%s' %(self.testparams['dataset']['dataset'],'chd')
        self.testparams['block']['block_name'] = '%s_%s' %(self.testparams['block']['block_name'],'chd')

        for i in range(len(self.testparams['file_conf_list'])):
            self.testparams['file_conf_list'][i]['lfn'] =  self.testparams['file_conf_list'][i]['lfn'].replace('.root','_child.root')
        
        self.api.insertBulkBlock(blockDump=self.testparams)
        print "Done inserting child files"

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientBlockWriter_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
