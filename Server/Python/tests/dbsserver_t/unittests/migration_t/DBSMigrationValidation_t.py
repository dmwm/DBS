"""
migration unit and validation tests
"""

__revision__ = "$Id: DBSMigrationValidation_t.py,v 1.1 2010/08/26 18:06:54 afaq Exp $"
__version__ = "$Revision: 1.1 $"

import os
import sys
import unittest
import time
#import uuid
from ctypes import *
from dbsserver_t.utils.DBSRestApi import DBSRestApi

# Source parameters
#srcurl = os.environ["DBS_SRC_URL"]
srcurl = "http://vocms09.cern.ch:8585/DBS"
srcdataset = "/RelValSinglePiPt100/CMSSW_3_1_0_pre9_IDEAL_31X_v1/GEN-SIM-DIGI-RAW-HLTDEBUG"

# Destination parameters
config = os.environ["DBS_TEST_CONFIG_WRITER"]
service = os.environ["DBS_TEST_SERVICE"]

api = DBSRestApi(config, 'MIGRATE')

class DBSMigrationValidation_t(unittest.TestCase):

        def setUp(self):
	    """setup all necessary parameters"""

	def test01(self):
	    """test01: able to register a migration request"""
	    data = dict(migration_url="http://vocms09.cern.ch:8585/DBS", migration_input='/RelValSinglePiPt100/CMSSW_3_1_0_pre9_IDEAL_31X_v1/GEN-SIM-DIGI-RAW-HLTDEBUG')
	    api.insert('submit', data)
	    ## validate that the request has been submitted
	    data = { 'dataset' : srcdataset }
	    request=api.list('status', data)
	    assert len(request) > 0
	    assert request[0]['migration_request_id'] > 0

	
