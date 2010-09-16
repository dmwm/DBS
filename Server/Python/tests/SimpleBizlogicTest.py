import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSProcessingEra import DBSProcessingEra
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile
from dbs.business.DBSRun import DBSRun


class Test:

    
    def __init__(self):
        #url = "oracle://user:password@host:port/sid"
	#url="oracle://anzar:XXXXXXX@uscmsdb03.fnal.gov:1521/cmscald"
	#self.owner="anzar"
	#url="mysql://dbs3:XXXXXXX@cmssrv49.fnal.gov:3306/CMS_DBS3"
	url="mysql://dbs3:XXXXXXX@cmssrv49.fnal.gov:3306/CMS_DBS3_ANZ_2"
	self.owner="__MYSQL__"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()

    def updateFileStatus(self):
	bo = DBSFile(self.logger, self.dbi, self.owner)
	import pdb
	pdb.set_trace()
	bo.updateStatus('/store/mc/Winter09/TTbar-madgraph/GEN-SIM-DIGI-RECO/IDEAL_V11_FastSim_v1/0060/0A83790D-71E1-DD11-9732-001EC9AAA058.root', 1)
    def testRun(self):
	bo = DBSRun(self.logger, self.dbi, self.owner)
	print bo.listRuns(minRun=5, maxRun=9)


    def testPrimary(self):
	
	bo = DBSPrimaryDataset(self.logger, self.dbi, self.owner)
	binput = {"primary_ds_type": "test", "primary_ds_name": "anzar_0002"}
	bo.insertPrimaryDataset(binput)
    
    def testOutputConfig(self):
	"""
	This method can be used to test OutputConfig Buisiness Object

	"""
        bo = DBSOutputConfig(self.logger, self.dbi, self.owner)
	binput = {'app_name': 'Re1-Repacker', 'release_version': 'CMSSW_12_1_8',  'pset_hash': 'N11O_PSET_HASH', 'output_module_label' : 'outmod_test_label_12', 'creation_date' : 1234, 'create_by' : 'anzar' }
        bo.insertOutputConfig(binput)


    def testDatasetInsert(self):
        """
        This method is being used for testing datasets's insert DAO
        """

	bo = DBSDataset(self.logger, self.dbi, self.owner)
        binput = {
			'is_dataset_valid': 1, 'primary_ds_name': 'TkCosmics38T', 'physics_group_name': 'Tracker', 'global_tag': 'STARTUP31X_V3::All',
                        'processed_ds_name': 'Summer09-STARTUP31X_V3-v2', 'dataset': '/TkCosmics38T/Summer09-STARTUP31X_V3-v2/GEN-SIM-DIGI-RAW',
                        'dataset_type': 'PRODUCTION', 'xtcrosssection': 123, 'data_tier_name': 'GEN-SIM-DIGI-RAW',
			'creation_date' : 1234, 'create_by' : '__MYSQL__', "last_modification_date" : 1234, "last_modified_by" : "anzar",
                        'output_configs' : [  {'app_name': 'Repacker', 'release_version': 'CMSSW_2_1_7',  'pset_hash': 'NO_PSET_HASH'}  ] 
		}

	binput = {'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': u'/QCD_BCtoMu_Pt20/Summer08_IDEAL_V9_v1/GEN-SIM-RAW', 
		    'dataset_type': 'PRODUCTION', 'processed_ds_name': u'Summer08_IDEAL_V9_v1', 'primary_ds_name': u'QCD_BCtoMu_Pt20', 
		    'output_configs': [{'release_version': u'CMSSW_2_1_7', 'pset_hash': u'NO_PSET_HASH', 'app_name': u'cmsRun', 'output_module_label': u'Merged'}, 
		    {'release_version': u'CMSSW_2_1_7', 'pset_hash': u'76e303993a1c2f842159dbfeeed9a0dd', 'app_name': u'cmsRun', 'output_module_label': u'output'}], 
		    'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': 'GEN-SIM-RAW',
		    'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		    #'processing_version': '1',  'acquisition_era_name': u'',
		}
	binput = {'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': u'/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW', 
		    'dataset_type': 'PRODUCTION', 'processed_ds_name': u'Summer09-STARTUP31X_V3-v1', 'primary_ds_name': u'TkCosmics38T', 
		    'data_tier_name': 'GEN-SIM-DIGI-RAW', 'global_tag': u'STARTUP31X_V3::All', 'xtcrosssection': 123, 'primary_ds_type': 'test', 
		    'output_configs': [
			    {'release_version': u'CMSSW_3_1_2', 'pset_hash': u'4847ed25a7e108a7b1e704a26f345aa8', 'app_name': u'cmsRun', 'output_module_label': u'Merged'}, 
			    {'release_version': u'CMSSW_3_1_2', 'pset_hash': u'NO_PSET_HASH', 'app_name': u'cmsRun', 'output_module_label': u'Merged'}, 
			    {'release_version': u'CMSSW_3_1_2', 'pset_hash': u'4847ed25a7e108a7b1e704a26f345aa8', 'app_name': u'cmsRun', 'output_module_label': u'output'}
			],
		    'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		}

        bo.insertDataset(binput)

    def testAcquisitionEra(self):
	        """
		This method is being used for testing AcquisitionEra insert biz
		"""
		bo = DBSAcquisitionEra(self.logger, self.dbi, self.owner)
		binput = {
			    'acquisition_era_name' : 'test_acq_001', 'creation_date' : 1234, 'create_by' : 'anzar'
			}
		bo.insertAcquisitionEra(binput)

    def testProcessingEra(self):
	        """
		This method is being used for testing AcquisitionEra insert biz
		"""
		bo = DBSProcessingEra(self.logger, self.dbi, self.owner)
		binput = {
		          'processing_version' : 'v001', 'creation_date' : 1234, 'create_by' : 'anzar', 'description' : 'anzar test description'
		         }
		bo.insertProcessingEra(binput)

    def testBlock(self):
		"""
		This method is used for testing Block insert Bi logic
		"""
		
		bo = DBSBlock(self.logger, self.dbi, self.owner)
		binput = {'block_name': u'/QCD_BCtoMu_Pt20/Summer08_IDEAL_V9_v1/GEN-SIM-RAW#f930d82a-f72b-4f9e-8351-8a3cb0c43b79', 'file_count': u'100', 
			    'origin_site': u'cmssrm.fnal.gov', 'last_modification_date': u'1263231733', 
			    'create_by': u'/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'block_size': u'228054411650', 
			    'open_for_writing': 1, 'last_modified_by': u'anzar@cmssrv49.fnal.gov', 'creation_date': u'1228050132'}
		bo.insertBlock(binput)

    def testFiles(self):
		"""
		This method tests the buisness login (and dao ) for the insertFiles() API
		"""

		bo = DBSFile(self.logger, self.dbi, self.owner)
    
		binput = [
		{'adler32': u'NOTSET', 'file_type': 'EDM', 'file_output_config_list': [{'release_version': 'CMSSW_1_2_3', 'pset_hash': '76e303993a1c2f842159dbfeeed9a0dd', 'app_name': 
		'cmsRun', 'output_module_label': 'Merged'}], 'dataset': '/unittest_web_primary_ds_name_684/unittest_web_dataset_684/GEN-SIM-RAW', 
		'file_size': u'2012211901', 'auto_cross_section': 0.0, 'check_sum': u'1504266448', 
		'file_lumi_list': [{'lumi_section_num': u'27414', 'run_num': u'1'}, 
				    {'lumi_section_num': u'26422', 'run_num': u'1'}, 
				    {'lumi_section_num': u'29838', 'run_num': u'1'}], 
				    'file_parent_list': [], 'event_count': u'1619', 'logical_file_name': 
					'/store/mc/parent_684/0.root', 
		'block': '/unittest_web_primary_ds_name_684/unittest_web_dataset_684/GEN-SIM-RAW#684',
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		}
		]
		bo.insertFile(binput)
	

test=Test()
#test.testPrimary()
#test.testOutputConfig()
#test.testDatasetInsert()
#test.testAcquisitionEra()
#test.testProcessingEra()
#test.testBlock()
#test.testFiles()
#test.testRun()
test.updateFileStatus()
    
