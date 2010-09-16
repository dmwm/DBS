import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSProcessingEra import DBSProcessingEra
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
	binput = {'app_name': 'Repacker', 'version': 'CMSSW_2_1_7',  'hash': 'NO_PSET_HASH', 'output_module_label' : 'outmod_test_label', 'creation_date' : 1234, 'create_by' : 'anzar' }
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
                        'output_configs' : [  {'app_name': 'Repacker', 'version': 'CMSSW_2_1_7',  'hash': 'NO_PSET_HASH'}  ] 
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


    def testFiles(self):
		"""
		This method tests the buisness login (and dao ) for the insertFiles() API
		"""

		bo = DBSFile(self.logger, self.dbi, "anzar")
		binput = [{ 
			    'file_type': 'EDM', 
			    'logical_file_name': '/store100077/mc/Summer09/TkCosmics38T/GEN-SIM-DIGI-RAW/STARTUP31X_V3-v1/0010/66EE7132-FFB3-DE11-9D33-001E682F1FA6.root', 
			    'file_size': '2824329131', 
			    'last_modification_date': '1255099729', 
			    'file_parent_list': [], 
			    'auto_cross_section': 0.0, 
			    'md5': 'NOTSET', 
			    'check_sum': '862355611', 
			    'file_lumi_list': [{'LUMI_SECTION_NUM': u'10018', 'RUN_NUM': '1'}], 
			    'adler32': 'NOTSET', 
			    'event_count': '2041', 
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
test.testFiles()
    
