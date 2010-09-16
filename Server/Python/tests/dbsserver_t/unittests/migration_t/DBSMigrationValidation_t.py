"""
migration unit and validation tests
"""

__revision__ = "$Id: DBSMigrationValidation_t.py,v 1.2 2010/08/27 14:19:48 afaq Exp $"
__version__ = "$Revision: 1.2 $"

import os
import sys
import unittest
import time
import logging
import threading 
from ctypes import *

import json, cjson
import urllib, urllib2

from dbsserver_t.utils.DBSRestApi import DBSRestApi

from WMCore.Configuration import Configuration, loadConfigurationFile
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from sqlalchemy import create_engine

from dbs.components.migration.DBSMigrationEngine import DBSMigrationEngine

def getRemoteData(url, verb, searchingName, searchingVal):
    """Client type call to get content from the remote server"""
    try:
	resturl = "%s/%s?%s=%s" % (url, verb, searchingName, searchingVal)
	req = urllib2.Request(url = resturl)
        data = urllib2.urlopen(req)
        #Now it is depending on the remote server's calls.
        ddata = cjson.decode(data.read())
    except Exception, ex:
	#print ex
	raise Exception ("Unable to get information from src dbs : %s for %s?%s=%s" %(url, verb, searchingName, searchingVal))
    return ddata
				

def setupDB(config):
    logger = logging.getLogger()
    _engineMap = {}
    myThread = threading.currentThread()
    myThread.logger=logger
    myThread.dbFactory = DBFactory(myThread.logger, config.CoreDatabase.connectUrl, options={})
    myThread.dbi = myThread.dbFactory.connect()
    
# Source parameters
#srcurl = os.environ["DBS_SRC_URL"]
srcurl = "http://vocms09.cern.ch:8585/DBS"
srcdataset = "/RelValSinglePiPt100/CMSSW_3_1_0_pre9_IDEAL_31X_v1/GEN-SIM-DIGI-RAW-HLTDEBUG"

# Destination parameters
readerconfig = os.environ["DBS_TEST_CONFIG_READER"]
writerconfig = os.environ["DBS_TEST_CONFIG_WRITER"]
service = os.environ["DBS_TEST_SERVICE"]
readerapi = DBSRestApi(readerconfig, service)
migapi = DBSRestApi(writerconfig, 'MIGRATE')

class DBSMigrationValidation_t(unittest.TestCase):

        def setUp(self):
	    """setup all necessary parameters"""

	def test01(self):
	    """test01: able to register a migration request"""
	    data = dict(migration_url="http://vocms09.cern.ch:8585/DBS", migration_input='/RelValSinglePiPt100/CMSSW_3_1_0_pre9_IDEAL_31X_v1/GEN-SIM-DIGI-RAW-HLTDEBUG')
	    migapi.insert('submit', data)
	    ## validate that the request has been submitted
	    data = { 'dataset' : srcdataset }
	    request=migapi.list('status', data)
	    assert len(request) > 0
	    assert request[0]['migration_request_id'] > 0
	    
	def test02(self):
	    """test02 : start migration and finish it"""
	    cfg=loadConfigurationFile(writerconfig)
	    setupDB(cfg)
	    migrator=DBSMigrationEngine(cfg)
	    migrator.setup("NONE")
	    migrator.algorithm("NONE")
	    
	def test03(self):	
	    """test03 : validate that migratee and migrated blocks are SAME"""
	    #SRC
	    # get list of blocks from source
	    srcblklist = getRemoteData(srcurl, "blocks", "dataset", srcdataset)
	    for asrcblk in srcblklist:
		srcblkdump = getRemoteData(srcurl, "blockdump", "block_name", asrcblk['block_name'].replace("#",urllib.quote_plus('#')) )
		dstblkdump = readerapi.list('blockdump', block_name=asrcblk['block_name'] )
		assert len(srcblkdump['file_conf_list']) == len(dstblkdump['file_conf_list'])
		assert len(srcblkdump['files']) == len(dstblkdump['files'])
		#for afile in srcblkdump['files']:
		#    assert afile[''] = dstblkdump['files']
		assert len(srcblkdump['block_parent_list']) == len(dstblkdump['block_parent_list'])
		assert srcblkdump['processing_era'] == dstblkdump['processing_era']
		assert len(srcblkdump['ds_parent_list']) == len(dstblkdump['ds_parent_list'])
		assert srcblkdump['primds']['primary_ds_name'] == dstblkdump['primds']['primary_ds_name']
		assert srcblkdump['acquisition_era'] == dstblkdump['acquisition_era']
		assert srcblkdump['dataset']['dataset'] == dstblkdump['dataset']['dataset']
		assert len(srcblkdump['file_parent_list']) == len(dstblkdump['file_parent_list'])
		
	def test04(self):
	    """test04: lets attempt remigration of the same dataset, it should not raise any errors"""
	    data = dict(migration_url="http://vocms09.cern.ch:8585/DBS", migration_input='/RelValSinglePiPt100/CMSSW_3_1_0_pre9_IDEAL_31X_v1/GEN-SIM-DIGI-RAW-HLTDEBUG')
	    migapi.insert('submit', data)

	    cfg=loadConfigurationFile(writerconfig)
	    setupDB(cfg)
	    migrator=DBSMigrationEngine(cfg)
	    migrator.setup("NONE")
	    migrator.algorithm("NONE")    
			
