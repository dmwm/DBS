#!/usr/bin/env python
"""
DBS Migration Service Polling Module
"""
__revision__ = "$Id: DBSMigrationServicePoller.py,v 1.4 2010/06/29 19:22:36 afaq Exp $"
__version__ = "$Revision: 1.4 $"

import threading
import logging
import traceback
import os
import time

from sqlalchemy import exceptions

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory

import json, cjson
import urllib, urllib2

class DBSMigrationServicePoller(BaseWorkerThread) :

    """
    Handles poll-based migration requests
    """
    
    def __init__(self, config):
        """
        Initialise class members
        """

	# Used for creating connections/transactions
        myThread = threading.currentThread()
	self.dbi = myThread.dbi

	self.logger = myThread.logger
	
	BaseWorkerThread.__init__(self)
	# get the db owner
        self.config  = config
	dbconfig = config.section_("CoreDatabase")
	self.dbowner=dbconfig.dbowner

    # This is only called once by the frwk
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

	# Setup the DAO objects
	daofactory = DAOFactory(package='dbs.dao', logger=self.logger, dbinterface=self.dbi, owner=self.dbowner)

        self.sm		    = daofactory(classname="SequenceManager")
	self.reqlist = daofactory(classname="MigrationRequests.List")
	self.minreqlist = daofactory(classname="MigrationRequests.ListOldest")
	self.requestup   = daofactory(classname="MigrationRequests.Update")
	self.blklist = daofactory(classname="MigrationBlock.List")
	self.blkup = daofactory(classname="MigrationBlock.Update")

        self.primdslist	    = daofactory(classname="PrimaryDataset.List")
	self.datasetlist    = daofactory(classname="Dataset.List")
	self.blocklist	    = daofactory(classname="Block.List")
	self.filelist	    = daofactory(classname="File.List")
	self.fplist	    = daofactory(classname="FileParent.List")
        self.fllist	    = daofactory(classname="FileLumi.List")
	self.primdstpid	    = daofactory(classname='PrimaryDSType.GetID')
	self.tierid	    = daofactory(classname='DataTier.GetID')
        self.datatypeid	    = daofactory(classname='DatasetType.GetID')
        self.phygrpid	    = daofactory(classname='PhysicsGroup.GetID')
        self.procdsid	    = daofactory(classname='ProcessedDataset.GetID')
        self.procdsin	    = daofactory(classname='ProcessedDataset.Insert')
        self.primdsin	    = daofactory(classname="PrimaryDataset.Insert")
        self.datasetin	    = daofactory(classname='Dataset.Insert')
	
	self.sm = daofactory(classname = "SequenceManager")
	self.filein = daofactory(classname = "File.Insert")
	self.flumiin = daofactory(classname = "FileLumi.Insert")
	self.fparentin = daofactory(classname = "FileParent.Insert")
	self.fpbdlist = daofactory(classname = "FileParentBlock.List")
	self.blkparentin = daofactory(classname = "BlockParent.Insert")
	self.dsparentin = daofactory(classname = "DatasetParent.Insert")
	self.blkstats = daofactory(classname = "Block.ListStats")
	self.blkstatsin = daofactory(classname = "Block.UpdateStats")
	self.fconfigin = daofactory(classname='FileOutputMod_config.Insert')
	self.bufdeletefiles = daofactory(classname="FileBuffer.DeleteFiles")
	self.buflist = daofactory(classname="FileBuffer.List")
	self.buflistblks = daofactory(classname="FileBuffer.ListBlocks")
	self.buffinddups = daofactory(classname="FileBuffer.FindDuplicates")
	self.bufdeletedups = daofactory(classname="FileBuffer.DeleteDuplicates")
	self.compstatusin = daofactory(classname="ComponentStatus.Insert")
	self.compstatusup = daofactory(classname="ComponentStatus.Update")

	# Report that service has started
	self.insertStatus("STARTED")

    # This is the actual poll method, called over and over by the frwk
    def algorithm(self, parameters):
	"""
	Performs the handleMigration method, called by frwk over and over (until terminate is called)
	"""
	logging.debug("Running dbs migration algorithm")
	self.handleMigration()	
	
    def handleMigration(self):
	"""
	The actual handle method for performing migration

	* The method takes a request and tries to complete it till end, this way we 
	* do not have incomplete running migrations running forever
	
	1. get a migration request in 'PENDING' STATUS
	2. Change its status to 'RUNNING'
	3. Get the highest order 'PENDING' block
	4. Change the status of block to RUNNING
	5. Migrate it
	6. Change the block status to 'COMPLETED' (?remove from the list?)
	7. Pick the next block, go to 4.
	8. After no more blocks can be migrated, mark the request as DONE (move to history table!!!!)
	"""
	
	request={}
	request_id=-1
	try :
	    #1.
	    #FIXME: lock requests table
	    conn = self.dbi.connection()
	    request=self.getMigrationRequest(conn)
	    if len(request) < 1:
		return
	    request_id=request["migration_request_id"]
	    #If a request has been found
	    blocks = self.findMigrateableBlocks(conn, request_id)
	    for ablock in blocks:
	        self.migrateBlock(conn, ablock['migration_block'], request["migration_url"])
	        # report status to database, a cycle has been completed, successfully
	        self.reportStatus("WORKING FINE")
	    #Mark the request as done
	    self.updateRequestStatus(conn, request_id, "COMPLETED")
	    #FIXME: What do we do to the blocks, once all the blocks are DONE 
	except Exception, ex:
	    self.logger.exception("DBS Migration Service failed to perform migration %s" %str(ex))
	    if request.has_key('migration_request_id') and  request['migration_status'] == "RUNNING":
		self.updateRequestStatus(conn, request_id, "FAILED")
	    raise
	finally:
	    #FIXME: un-lock requests table
	    print "Unlock here"
	    conn.close()

    def getMigrationRequest(self, conn):
	"""
	Pick up a pending request from the queued requests (in database)
	--atomic operation
	"""
	request=[]
	try:
	    requests = self.minreqlist.execute(conn)
	    if len(requests) > 0:
		# get details
		request=self.reqlist.execute(conn, migration_request_id=requests[0]['migration_request_id'])[0]
		# 2.
		#Change the status to RUNNING
		self.updateRequestStatus(conn, request["migration_request_id"], "RUNNING")	
		request["migration_status"]="RUNNING"
	    return request
	except:
	    self.logger.exception("DBS Migrate Service Failed to find migration requests")
	    raise
	
    def updateRequestStatus(self, conn, request_id, status):
            try:
	        upst = dict(migration_status=status, migration_request_id=request_id)
                self.requestup.execute(conn, upst)
            except:
	        raise
		
    def updateBlockStatus(self, conn, block_name, status):
            try:
	        blkupst = dict(migration_status=status, migration_block=block_name)
                self.blkup.execute(conn, blkupst)
            except:
	        raise

    def findMigrateableBlocks(self, conn, request_id):
	"""
	Get the blocks that need to be migrated
	"""
	try:
	    result = self.blklist.execute(conn, request_id)
            return result
	except Exception, ex:
	    raise ex

    def migrateBlock(self, conn, block_name, url):
	"""
	Performs the block migration
	"""

	try:
	    self.updateBlockStatus(conn, block_name, 'RUNNING')
	    blockcontent = self.getBlock(url, block_name)
	    self.putBlock(blockcontent)
	    self.updateBlockStatus(conn, block_name, 'COMPLETED')
	except Exception, ex:
	    self.dbsMigrate.updateBlockStatus(conn, block_name, 'FAILED')
	    raise Exception ("Migration Failed, DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )

    def getBlock(self, url, block_name):
	"""Client type call to get the block content from the server"""
	blockname = block_name.replace("#",urllib.quote_plus('#'))
	resturl = "%s/blockdump?block_name=%s" % (url, blockname)
	req = urllib2.Request(url = resturl)
        data = urllib2.urlopen(req)
        ddata = cjson.decode(data.read())
        return ddata

    def putBlock(self, blockcontent):
	"""Huge method that inserts everything.
	   Want to do it in one transaction, so that if there is problem it rolls back.
	   Inserts all data into corresponding tables"""
	conn = self.dbi.connection()
        tran = conn.begin()

	#insert primary dataset
	d = blockcontent["primds"]
	primds = {}
	try:
	    primds["primary_ds_type_id"] = self.primdstpid.execute(conn, d["primary_ds_type"])
	    primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS", tran)
	    for k in ("primary_ds_name", "creation_date", "create_by"):
		primds[k] = d[k]

	    self.primdsin.execute(conn, primds, tran)
	except IntegrityError:
	    pass
	except:
	    tran.rollback()
	    conn.close()
	    raise

	#insert dataset (and processed dataset if it is not already inserted)
	d = blockcontent["dataset"]
	dataset = {}
	dataset["primary_ds_id"]    = primds["primary_ds_id"]
	dataset["data_tier_id"]     = self.tierid.execute(conn, d["data_tier_name"])
	dataset["dataset_type_id"]  = self.datatypeid.execute(conn, d["dataset_type"])
	dataset["physics_group_id"] = self.phygrpid.execute(conn, d["physics_group_name"])

	procid = self.procdsid.execute(conn, d["processed_ds_name"])
	if procid>0:
	    dataset["processed_ds_id"] = procid
	else:
	    procid = self.sm.increment(conn, "SEQ_PSDS", tran)
	    procdaoinput = {"processed_ds_name":d["processed_ds_name"],
                            "processed_ds_id":procid}
	    self.procdsin.execute(conn, procdaoinput, tran)
	    dataset["processed_ds_id"] = procid
        dataset["dataset_id"] = self.sm.increment(conn, "SEQ_DS", tran) 
	for k in ("dataset", "is_dataset_valid", "global_tag", "xtcrosssection",
		  "creation_date", "create_by", "last_modification_date", "last_modified_by"):
	    print "DO SOMETHING EHRE............"
	
    def insertStatus(self, status = "UNKNOWN" ):
	"""
	This is a local function, basically component reports its status to database
	"""
	try:
	    conn = self.dbi.connection()
	    tran = conn.begin()
	    comp_status_id = self.sm.increment(conn, "SEQ_CS", transaction=tran)
	    statusObj={ "comp_status_id" : comp_status_id, "component_name" : "MIGRATION SERVICE", "component_status" : status, "last_contact_time" : str(int(time.time())) }
	    self.compstatusin.execute(conn, statusObj, tran)
	    tran.commit()
	except exceptions.IntegrityError, ex:
		    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
			self.logger.warning("Component Already Known to DBS, ignoring...")
		    else:
			tran.rollback()
			self.logger.exception("DBS Insert Buffer Poller Exception: %s" %ex)
			raise
	except Exception, ex:
	    tran.rollback()
	    self.logger.exception("DBS Insert Buffer Poller Exception: %s" %ex)
	    raise
    	finally:
	    conn.close()

    def reportStatus(self, status = "UNKNOWN" ):
	"""
	This is a local function, basically component reports its status to database
	"""
	try:
	    conn = self.dbi.connection()
	    tran = conn.begin()
	    statusObj={ "component_name" : "WRITER BUFFER", "component_status" : status, "last_contact_time" : str(int(time.time())) }
	    self.compstatusup.execute(conn, statusObj, tran)
	    tran.commit()
	except Exception, ex:
	    tran.rollback()
	    self.logger.exception("DBS Insert Buffer Poller Exception: %s" %ex)
    	finally:
	    conn.close()
        # called by frk at the termination time

    def terminate(self, params):
	"""
	Terminate
	"""
	logging.debug("Terminating DBS Migration Service")


if __name__ == '__main__':
#----------------
# This section is just for testing
#---------------

    from WMCore.Configuration import Configuration, loadConfigurationFile
    from WMCore.Database.DBCore import DBInterface
    from WMCore.Database.DBFactory import DBFactory
    from sqlalchemy import create_engine
    import logging

    def configure(configfile):
        config = loadConfigurationFile(configfile)
	
	"""
        wconfig = cfg.section_("Webtools")
        app = wconfig.application
        appconfig = cfg.section_(app)
        service = list(appconfig.views.active._internal_children)[0]
        dbsconfig = getattr(appconfig.views.active, service)
        dbsconfig.formatter.object="WMCore.WebTools.RESTFormatter"
        config = Configuration()
        config.component_('DBS')
        config.DBS.application = app
        config.DBS.model       = dbsconfig.model
        config.DBS.formatter   = dbsconfig.formatter
        config.DBS.database    = dbsconfig.database
        config.DBS.dbowner     = dbsconfig.dbowner
        config.DBS.version     = dbsconfig.version
        config.DBS.default_expires = 300
        config = config.section_("DBS")
	"""

        print config
        return config

    def setupDB(config):
	
	logger = logging.getLogger()
	_engineMap = {}
	myThread = threading.currentThread()
	print config
	"""
	_defaultEngineParams = {"convert_unicode" : True,
                            "strategy": "threadlocal",
                            "pool_recycle": 7200}
	engine = _engineMap.setdefault(config.database.connectUrl,
                                         create_engine(config.database.connectUrl,
                                                       connect_args = {},
                                                       **_defaultEngineParams)
                                                  )
	"""

	myThread.logger=logger
	myThread.dbFactory = DBFactory(myThread.logger, config.CoreDatabase.connectUrl, options={})
        myThread.dbi = myThread.dbFactory.connect()
    	#dbInterface =  DBInterface(logger, engine)
        #myThread.dbi=dbInterface
#Run the test

    config=configure("DefaultConfig.py")
    setupDB(config)

    migrator=DBSMigrationServicePoller(config)
    migrator.setup("NONE")
    migrator.algorithm("NONE")
