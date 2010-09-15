import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSOutputConfig import DBSOutputConfig

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

test=Test()
test.testOutputConfig()

