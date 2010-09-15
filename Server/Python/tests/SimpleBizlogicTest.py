import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSDataset import DBSDataset

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

test=Test()
#test.testOutputConfig()
test.testDatasetInsert()


