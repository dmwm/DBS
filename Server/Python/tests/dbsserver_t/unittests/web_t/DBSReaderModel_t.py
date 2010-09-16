"""
web unittests
"""

__revision__ = "$Id: DBSReaderModel_t.py,v 1.19 2010/07/09 18:34:51 yuyi Exp $"
__version__ = "$Revision: 1.19 $"

import os, sys, imp
import json
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from DBSWriterModel_t import outDict

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

#config = os.environ["DBS_TEST_CONFIG_READER"]
config = os.environ["DBS_TEST_CONFIG_WRITER"]
api = DBSRestApi(config)

class DBSReaderModel_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""
        global testparams
        if len(testparams) == 0:
            testparams = outDict
	#import pdb
	#pdb.set_trace()
    def test01(self):
        print '\n Test01 web.DBSReaderModel.listPrimaryDatasets: basic test'
        api.list('primarydatasets')

    def test02(self):
	print '\n Test02 web.DBSReaderModel.listPrimaryDatasets: basic test'
	api.list('primarydatasets', primary_ds_name='*')
    
    def test03(self):
	print "\n Test03 web.DBSReaderModel.listPrimaryDatasets: basic test"
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name'])

    def test04(self):
        print "\n Test04 web.DBSReaderModel.listPrimaryDatasets: basic test"
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name']+'*')
       
    def test05(self):
        print "\n Test05 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets')
    
    def test06(self):
        print "\n Test06 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset='*')

    def test106(self):
        print "\n Test106 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', run_num=testparams['run_num'])

    def test07(self):
        print "\n Test07 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset=testparams['dataset'])

    def test107(self):
        print "\n Test107 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', primary_ds_type=testparams['primary_ds_type'])

    def test108(self):
        print "\n Test108 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets',  primary_ds_name=testparams['primary_ds_name'])

    def test08(self):
        print "\n Test08 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset=testparams['dataset']+'*')

    def test09(self):
        print "\n Test09 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', parent_dataset='*')

    def test109(self):
        print "\n Test109 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', data_tier_name='*RAW*')   
 
    def test10(self):
        print "\n Test10 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', release_version='*')

    def test110(self):
        print "\n Test110 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', physics_group_name='QCD')

    def test11(self):
        print "\n Test11 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', release_version=testparams['release_version'])

    def test111(self):
        print "\n Test111 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset_access_type='READONLY')

    def test12(self):
        print "\n Test12 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', release_version=testparams['release_version']+'*')

    def test112(self):
        print "\n Test112 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', logical_file_name=testparams['files'][0])

    def test13(self):
        print "\n Test13 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', pset_hash='*')

    def test14(self):
        print "\n Test14 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', pset_hash=testparams['pset_hash'])

    def test15(self):
        print "\n Test15 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', app_name='*')

    def test16(self):
        print "\n Test16 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', app_name=testparams['app_name'])
    
    def test17(self):
        print "\n Test17 web.DBSReaderModel.listDatasets: basic test"
	api.list('datasets', output_module_label='*')

    def test18(self):
        print "\n Test18 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', output_module_label=testparams['output_module_label'])

    def test19(self):
        print "\n Test19 web.DBSReaderModel.listDatasets: basic test"
	api.list('datasets', dataset=testparams['dataset'], 
                                  parent_dataset='*',
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test20(self):
        print "\n Test20 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset=testparams['dataset'],
                                  release_version=testparams['release_version']
                                  )

    def test21(self):
        print "\n Test21 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  )

    def test22(self):
        print "\n Test22 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', app_name=testparams['app_name'],
                             output_module_label=testparams['output_module_label'])

    def test23(self):
        print "\n Test23 web.DBSReaderModel.listDatasets: basic test"
        api.list('datasets', dataset=testparams['dataset'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])
    def test24(self):
        print "\n Test24 web.DBSReaderModel.listBlocks: basic negative test"
	try:
	    api.list('blocks', dataset='*')
        except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test25(self):
        print "\n Test25 web.DBSReaderModel.listBlocks: basic test"
	api.list('blocks', dataset=testparams['dataset'])

    def test26(self):
        print "\n Test26 web.DBSReaderModel.listBlocks: basic test"
        api.list('blocks', block_name=testparams['block'])

    def test27(self):
        print "\n Test27 web.DBSReaderModel.listBlocks: basic negative test"
	try:
	    api.list('blocks', origin_site_name=testparams['site'])
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
    
    def test127(self):
	print "\n Test127 web.DBSReaderModel.listBlocks: basic test"
	api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'])
	

    def test28(self):
        print "\n Test28 web.DBSReaderModel.listBlocks: basic negative test"
        try:
            api.list('blocks', block_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test29(self):
        print "\n Test29 web.DBSReaderModel.listBlocks: basic negative test"
        try:
            api.list('blocks', origin_site_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")
    
    def test129(self):
	 print "\n Test129 web.DBSReaderModel.listBlocks: basic test"
	 api.list('blocks', dataset=testparams['dataset'],
	           origin_site_name=testparams['site'])
		   
    def test130(self):
	print "\n Test130 web.DBSReaderModel.listBlocks: basic test"
	api.list('blocks', dataset=testparams['dataset'],
		origin_site_name=testparams['site'])

    def test30(self):
        print "\n Test30 web.DBSReaderModel.listBlocks: basic test"
        api.list('blocks', dataset=testparams['dataset'],
                                block_name=testparams['block'],
                                origin_site_name=testparams['site'])
        
    def test31(self):
        print "\n Test31 web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."
	
        try:
	    api.list('blocks')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised.")
            
    def test32(self):
        print "\n Test32 web.DBSReaderModel.listFiles: basic negative test"
	try:
	    api.list('files', dataset='*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test132(self):
	print "\n Test132 web.DBSReaderModel.listFiles: basic test"
	api.list('files', dataset=testparams['dataset'], minrun=1, maxrun=testparams['run_num'])
    
    def test33(self):
        print "\n Test33 web.DBSReaderModel.listFiles: basic test"
	api.list('files', dataset=testparams['dataset'])

    def test133(self):
        print "\n Test133 web.DBSReaderModel.listFiles: basic test"
        api.list('files', dataset=testparams['dataset'], lumi_list=[1,2,3], maxrun=testparams['run_num'], 
		  minrun=testparams['run_num'])

    def test34(self):
        print "\n Test34 web.DBSReaderModel.listFiles: basic negative test"
	try:
	    api.list('files', dataset=testparams['dataset']+'*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test134(self):
        print "\n Test134 web.DBSReaderModel.listFiles: basic test"
        api.list('files', dataset=testparams['dataset'], origin_site_name=testparams['site'])


    def test35(self):
        print "\n Test35 web.DBSReaderModel.listFiles: basic negative test"
	try:
	    api.list('files', block_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test35(self):
        print "\n Test35 web.DBSReaderModel.listFiles: basic test"
	api.list('files', block_name=testparams['block'])

    def test72(self):
        print "\n Test72 web.DBSReaderModel.listFiles: basic negative test"
	try:
	    api.list('files', logical_file_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test36(self):
        print "\n Test36 web.DBSReaderModel.listFiles: basic test"
	#need to be updated
	#print testparams['files']
	lfn= testparams['files'][1]
	api.list('files', logical_file_name=lfn)	

    def test37(self):
        print "\n Test37 web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed."
        try: api.list('files')
        except: pass
        else: self.fail("Exception was expected and was not raised")
       
    def test38(self):
        print "\n Test38 web.DBSReaderModel.listDatasetParents: basic test"
        api.list('datasetparents', dataset="*")

    def test39(self):
        print "\n Test39 web.DBSReaderModel.listDatasetParents: basic test"
        api.list('datasetparents', dataset=testparams['dataset'])

    def test40(self):
        print "\n Test40 web.DBSReaderModel.listDatasetParents: basic test"
        api.list('datasetparents', dataset=testparams['dataset']+'*')
        
    def test41(self):
        print "\n Test41 web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed"
        try: 
	    api.list('datasetparents')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised")
            
    def test42(self):
        print "\n Test42 web.DBSReaderModel.listOutputConfigs: basic test"
	api.list('outputconfigs')
    
    def test43(self):
        print "\n Test43 web.DBSReaderModel.listOutputConfigs: basic test"
	api.list('outputconfigs', dataset="*")

    def test44(self):
        print "\n Test44 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', dataset=testparams['dataset'])

    def test45(self):
        print "\n Test45 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', dataset=testparams['dataset']+"*")
	
    def test46(self):
        print "\n Test46 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', logical_file_name="*")

    def test47(self):
        print "\n Test47 web.DBSReaderModel.listOutputConfigs: basic test"
	#need to be updated with LFN
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn)

    def test48(self):
        print "\n Test48 web.DBSReaderModel.listOutputConfigs: basic test" 
        #need to be updated with LFN 
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn+"*")

    def test49(self):
        print "\n Test49 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', release_version="*")

    def test50(self):
        print "\n Test50 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', release_version=testparams['release_version'])

    def test51(self):
        print "\n Test51 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', release_version=testparams['release_version']+'*')

    def test52(self):
        print "\n Test52 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', pset_hash="*")

    def test53(self):
        print "\n Test53 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', pset_hash=testparams['pset_hash'])

    def test54(self):
	print "\n Test54 web.DBSReaderModel.listOutputConfigs: basic test"
	api.list('outputconfigs', app_name="*")

    def test55(self):
        print "\n Test55 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', app_name=testparams['app_name'])

    def test56(self):
        print "\n Test56 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', app_name=testparams['app_name']+"*")

    def test56(self):
        print "\n Test56 web.DBSReaderModel.listOutputConfigs: basic test"
	api.list('outputconfigs', output_module_label="*")
 
    def test57(self):
        print "\n Test57 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', output_module_label=testparams['output_module_label'])

    def test58(self):
        print "\n Test58 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', output_module_label=testparams['output_module_label']+'*')

    def test60(self):
        print "\n Test60 web.DBSReaderModel.listOutputConfigs: basic test"
	api.list('outputconfigs', dataset=testparams['dataset'],
                                  logical_file_name="*",
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test61(self):
        print "\n Test61 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'],
                                  output_module_label=testparams['output_module_label'])

    def test62(self):
        print "\n Test62 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', logical_file_name="*",
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])
    def test63(self):
        print "\n Test63 web.DBSReaderModel.listOutputConfigs: basic test"
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'])

    def test64(self):
        print "\n Test64 web.DBSReaderModel.listFileParents: basic test"
        api.list('fileparents', logical_file_name="*")

    def test65(self):
        print "\n Test65 web.DBSReaderModel.listFileParents: basic test"
        api.list('fileparents', logical_file_name="ABC")
    
    def test66(self):
        print "\n Test66 web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed"
        try: api.list('fileparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
    def test66(self):
        print "\n Test66 web.DBSReaderModel.listFileLumis: basic test"
        api.list('filelumis', logical_file_name="*")

    def test67(self):
        print "\n Test67 web.DBSReaderModel.listFileLumis: basic test"
	#need to update LFN
	lfn= testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn)


    def test68(self):
        print "\n Test68 web.DBSReaderModel.listFileLumis: basic test"
        api.list('filelumis', block_name="*")

    def test69(self):
        print "\n Test69 web.DBSReaderModel.listFileLumis: basic test"
        api.list('filelumis', block_name=testparams['block'])

    def test70(self):
        print "\n Test70 web.DBSReaderModel.listFileLumis: basic test"
        api.list('filelumis', block_name=testparams['block']+'*')

    def test71(self):
        print '\n Test71 web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed'
        try: api.list('filelumis')
        except: pass
        else: self.fail("Exception was expected and was not raised")
 
    def test72(self):
        print '\n Test72 web.DBSReaderModel.listFile with maxrun, minrun: basic '
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, dataset=testparams['dataset'])

    def test73(self):
        print '\n Test73 web.DBSReaderModel.listFile with maxrun, minrun:basic '
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, block_name=testparams['block'])

    def test74(self):
        print '\n Test74 web.DBSReaderModel.listFile with original site: basic '
        api.list('files', origin_site_name=testparams['site'], dataset=testparams['dataset'])

    def test75(self):
        print '\n Test75 web.DBSReaderModel.listFile with original site: basic '
        api.list('files', origin_site_name=testparams['site'], block_name=testparams['block'])

    def test76(self):
        print '\n Test76 web.DBSReaderModel.listFile with config info: basic '
        api.list('files',  block_name=testparams['block'],
		release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test77(self):
        print '\n Test77 web.DBSReaderModel.listFile with config info: basic '
        api.list('files',  dataset=testparams['dataset'], 
                release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
                app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test78(self):
        print '\n Test78 web.DBSReaderModel.listDatasets with processing era: basic '
        api.list('datasets', processing_version=testparams['processing_version'],) 

    def test79(self):
        print '\n Test79 web.DBSReaderModel.listDatasets with acquisition era: basic '
        api.list('datasets', acquisition_era=testparams['acquisition_era']) 

    def test80(self):
        print '\n Test80 web.DBSReaderModel.listDataTypes : basic '
        api.list('datatypes' )

    def test81(self):
        print '\n Test81 web.DBSReaderModel.listDataTypes : basic '
        api.list('datatypes', dataset=testparams['dataset'] )

    def test82(self):
        print '\n Test82 web.DBSReaderModel.listDataTypes : basic '
        api.list('datatypes', datatype=testparams['primary_ds_type'] )

    def test83(self):
	print'\n Test83 test for listRun: basic' 
	api.list('runs', minrun=0, maxrun=testparams['run_num'])

    def test84(self):
        print'\n Test83 test for listRun: basic'
        api.list('runs', minrun=0, maxrun=testparams['run_num'], logical_file_name=testparams['files'][0])

    def test85(self):
        print'\n Test83 test for listRun: basic'
        api.list('runs', block_name=testparams['block'])

    def test86(self):
        print'\n Test83 test for listRun: basic'
        api.list('runs', dataset_name=testparams['dataset'])


 	
    def test87(self):
        """list dataset parents"""
        api.list('datasetparents', dataset=testparams['dataset'])
        
    def test88(self):
        """list dataset children"""
        api.list('datasetchildren', dataset=testparams['parent_dataset'])

    def test89(self):
        """list block parents"""
        api.list('blockparents', block_name=testparams['block'])

    def test90(self):
        """list block children"""
        api.list('blockchildren', block_name=testparams['parent_block'])
        
    def test91(self):
        """list file parents"""
        print api.list('fileparents', logical_file_name=testparams['files'][0])
        
    def test92(self):
        """list file children"""
        api.list('filechildren', logical_file_name=testparams['parent_files'][0])
	
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    infofile=open("info.dict","r")    
    testparams=importCode(infofile, "testparams", 0).info
    unittest.TextTestRunner(verbosity=2).run(SUITE)
else:
    testparams={}
