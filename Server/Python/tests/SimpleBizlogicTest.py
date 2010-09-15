import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSProcessingEra import DBSProcessingEra
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile

class Test:

    def __init__(self):
        #url = "oracle://user:password@host:port/sid"
	url="oracle://anzar:anzar_cms2009@uscmsdb03.fnal.gov:1521/cmscald"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()

    def testOutputConfig(self):
	"""
	This method can be used to test OutputConfig Buisiness Object

	"""
        bo = DBSOutputConfig(self.logger, self.dbi, "anzar")
	binput = {'app_name': 'Repacker', 'release_version': 'CMSSW_2_1_7',  'pset_hash': 'NO_PSET_HASH', 'output_module_label' : 'outmod_test_label', 'creation_date' : 1234, 'create_by' : 'anzar' }
        bo.insertOutputConfig(binput)


    def testDatasetInsert(self):
        """
        This method is being used for testing datasets's insert DAO
        """

	bo = DBSDataset(self.logger, self.dbi, "anzar")
        binput = {
			'is_dataset_valid': 1, 'primary_ds_name': 'TkCosmics38T', 'physics_group_name': 'Tracker', 'global_tag': 'STARTUP31X_V3::All',
                        'processed_ds_name': 'Summer09-STARTUP31X_V3-v2', 'dataset': '/TkCosmics38T/Summer09-STARTUP31X_V3-v2/GEN-SIM-DIGI-RAW',
                        'dataset_type': 'PRODUCTION', 'xtcrosssection': 123, 'data_tier_name': 'GEN-SIM-DIGI-RAW',
			'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
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

        bo.insertDataset(binput)

    def testAcquisitionEra(self):
	        """
		This method is being used for testing AcquisitionEra insert biz
		"""
		bo = DBSAcquisitionEra(self.logger, self.dbi, "anzar")
		binput = {
			    'acquisition_era_name' : 'test_acq_001', 'creation_date' : 1234, 'create_by' : 'anzar'
			}
		bo.insertAcquisitionEra(binput)

    def testProcessingEra(self):
	        """
		This method is being used for testing AcquisitionEra insert biz
		"""
		bo = DBSProcessingEra(self.logger, self.dbi, "anzar")
		binput = {
		          'processing_version' : 'v001', 'creation_date' : 1234, 'create_by' : 'anzar', 'description' : 'anzar test description'
		         }
		bo.insertProcessingEra(binput)

    def testBlock(self):
		"""
		This method is used for testing Block insert Bi logic
		"""
		
		bo = DBSBlock(self.logger, self.dbi, "anzar")
		binput = {'block_name': u'/QCD_BCtoMu_Pt20/Summer08_IDEAL_V9_v1/GEN-SIM-RAW#f930d82a-f72b-4f9e-8351-8a3cb0c43b79', 'file_count': u'100', 
			    'origin_site': u'cmssrm.fnal.gov', 'last_modification_date': u'1263231733', 
			    'create_by': u'/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'block_size': u'228054411650', 
			    'open_for_writing': 1, 'last_modified_by': u'anzar@cmssrv49.fnal.gov', 'creation_date': u'1228050132'}
		bo.insertBlock(binput)

    def testFiles(self):
		"""
		This method tests the buisness login (and dao ) for the insertFiles() API
		"""

		bo = DBSFile(self.logger, self.dbi, "anzar")
		binput = [{ 
			    'file_type': 'EDM', 
			    'logical_file_name': '/store110078/mc/Summer09/TkCosmics38T/GEN-SIM-DIGI-RAW/STARTUP31X_V3-v1/0010/66EE7132-FFB3-11-9D33-001E682F1FA68.root', 
			    'file_size': '2824329131', 
			    'last_modification_date': '1255099729', 
			    'file_parent_list': [
				    {"file_parent_lfn": "/store/data/Commissioning08/Monitor/RAW/v1/000/068/021/3678A458-E8A5-DD11-85D1-001617E30CC8.root"},
				    {"file_parent_lfn": "/store/data/Commissioning08/Monitor/RAW/v1/000/068/021/04C7DFE0-E4A5-DD11-A766-000423D99B3E.root"},
				    ], 
			    'auto_cross_section': 0.0, 
			    'md5': 'NOTSET', 
			    'check_sum': '862355611', 
			    'file_lumi_list': [{'lumi_section_num': 10018, 'run_num': 1}], 
			    'file_output_config_list' : [  {'app_name': 'Repacker', 'release_version': 'CMSSW_2_1_7',  'pset_hash': 'NO_PSET_HASH'}  ],
			    'adler32': 'NOTSET', 
			    'event_count': '2041', 
			    'creation_date' : 1234,
			    'create_by': 'cmsprod@caraway.hep.wisc.edu', 
			    'last_modified_by': '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 
			    'dataset': '/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW', 
			    'block': '/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#fc31bf9d-d44e-45a6-b87b-8fe6e2701062', 
			    'is_file_valid': 1
			}]

		bo.insertFile(binput)

test=Test()
#test.testOutputConfig()
#test.testDatasetInsert()
#test.testAcquisitionEra()
#test.testProcessingEra()
test.testBlock()
#test.testFiles()
    
