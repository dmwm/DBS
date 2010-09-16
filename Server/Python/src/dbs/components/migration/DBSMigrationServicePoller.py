#!/usr/bin/env python
"""
DBS Migration Service Polling Module
"""
__revision__ = "$Id: DBSMigrationServicePoller.py,v 1.3 2010/06/28 21:27:54 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import threading
import logging
import traceback
import os
import time

from sqlalchemy import exceptions

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory

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
	self.requestlist = daofactory(classname="MigrationRequests.ListOldest")
	self.requestup   = daofactory(classname="MigrationRequests.Update")









	
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



    def run(self):
	while True:
	    req = self.q.get()
	    migration_dataset = req['migration_dataset']
	    self.dbsMigrate.updateMigrationStatus('RUNNING', migration_dataset)
	    #here the actual migration goes
	    try:
		businput = dict(url=req["migration_url"], dataset=req["migration_dataset"])
		self.dbsMigrate.migrate(businput)
		self.dbsMigrate.updateMigrationStatus('COMPLETED', migration_dataset)
		time.sleep(10)
	    except Exception, ex:
		self.dbsMigrate.updateMigrationStatus('FAILED', migration_dataset)
		print "I AM HERE"
		raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )






	
    # called by frk at the termination time
    def terminate(self, params):
	"""
	Terminate
	"""
	logging.debug("Terminating DBS Migration Service")

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
	
	request=-1
	try :
	    #1.
	    #FIXME: lock requests table
	    requests=self.getMigrationRequests(conn)
	    if len(requests) > 0:
		request=requests[0]	
	    # 2.
	    #Change the status to RUNNING
	    self.updateRequestStatus(conn, request[""], "RUNNING")	
	except exception, ex:
	    self.logger.exception("DBS Migration Service failed to acquire a request: %s" %str(ex))
	    raise
	finally:
	    #FIXME: un-lock requests table

	#If a request has been found
	if request!=-1: 
	    self.findMigrateableBlocks()
	
    def getMigrationRequest(self):
	"""
	Pick up a pending request from the queued requests (in database)
	"""
	
	try:
	    conn = self.dbi.connection()
	    requests = self.requestlist.execute()
	    return requests
	except:
	    self.logger.exception("DBS Migrate Service Failed to find migration requests")
	    raise
	    
    def updateMigrationStatus(self, conn, migration_request_id, migration_status):
            try:
	        upst = dict(migration_status=migration_status, migration_request_id=migration_request_id)
                self.mgrup.execute(conn, upst)
            except:
	        raise
	
    def findMigrateableBlocks(self):
	"""
	Get the blocks that need to be migrated
	"""
	try:
	    conn = self.dbi.connection()
	    result = self.blklist.execute(conn)
            conn.close()
            return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()
    

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
    def getBufferedFiles(self, block_id):
        """
        Get some files from the insert buffer
        """
	    
	try:
	    conn = self.dbi.connection()
	    result = self.buflist.execute(conn, block_id)
            conn.close()
            return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()
   
    def removeDuplicates(self):
      """
      Check to see if there are duplicate entries for a block in the buffer, remove them
      """

      try:
	conn = self.dbi.connection()
	dups = self.buffinddups.execute(conn)
	if len(dups) > 0 :
	    # If there are duplicates, delete them
	    tran = conn.begin()
	    self.bufdeletedups.execute(conn, dups, tran)
	    tran.commit()
      except Exception, ex:
        tran.rollback()
        self.logger.exception(ex)
	raise ex
      finally:
        conn.close()
	
    def insertBufferedFiles(self, businput):
	"""

	insert the files from the buffer
	The files contain everything needed to insert them into various tables
	"""
	
	conn = self.dbi.connection()
	tran = conn.begin()
	block_id=""
	try:
	    files=[]
	    lumis=[]
	    parents=[]
	    configs=[]	    
	    fidl=[]
	    flfnl=[]
	    fileInserted=False
	    
	    for ablob in businput:
		block_id=ablob["file"]["block_id"]
		if ablob.has_key("file") : 
		    files.append(ablob["file"])
		    fidl.append(ablob["file"]["file_id"])
		    flfnl.append({"lfn" : ablob["file"]["logical_file_name"] })
	        if ablob.has_key("file_lumi_list") : lumis.extend(ablob["file_lumi_list"])
		if ablob.has_key("file_parent_list") : parents.extend(ablob["file_parent_list"])
		if ablob.has_key("file_output_config_list") : configs.extend(ablob["file_output_config_list"]) 

	    # insert files
	    if len(files) > 0:	
		self.filein.execute(conn, files, transaction=tran)
		fileInserted=True
	    # insert file - lumi   
	    if len(lumis) > 0:
		self.flumiin.execute(conn, lumis, transaction=tran)
	    # insert file parent mapping
	    if len(parents) > 0:
		self.fparentin.execute(conn, parents, transaction=tran)
	    # insert output module config mapping
	    if len(configs) > 0:
		self.fconfigin.execute(conn, configs, transaction=tran)  
	    # List the parent blocks and datasets of the file's parents (parent of the block and dataset)
	    # fpbdlist, returns a dict of {block_id, dataset_id} combination
	    if fileInserted:
		fpblks=[]
		fpds=[]
		fileParentBlocksDatasets = self.fpbdlist.execute(conn, fidl, transaction=tran)
		for adict in fileParentBlocksDatasets:
		    if adict["block_id"] not in fpblks:
			fpblks.append(adict["block_id"])
		    if adict["dataset_id"] not in fpds:
		    	fpds.append(adict["dataset_id"])
		# Update Block parentage
		if len(fpblks) > 0 :
		    # we need to bulk this, number of parents can get big in rare cases
		    bpdaolist=[]
		    iPblk = 0
		    fpblkInc = 10
		    bpID = self.sm.increment(conn, "SEQ_BP", transaction=tran, incCount=fpblkInc)
		    for ablk in fpblks:
			if iPblk == fpblkInc:
			    bpID = self.sm.increment(conn, "SEQ_BP", transaction=tran, incCount=fpblkInc)
			    iPblk = 0
			bpdao={ "this_block_id": block_id }
			bpdao["parent_block_id"] = ablk
			bpdao["block_parent_id"] = bpID
			bpdaolist.append(bpdao)
		    # insert them all
		    # Do this one by one, as its sure to have duplicate in dest table

		    for abp in bpdaolist:
			try:
			    self.blkparentin.execute(conn, abp, transaction=tran)
			except exceptions.IntegrityError, ex:
			    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
				pass
			    else:
				raise
		# Update dataset parentage
		if len(fpds) > 0 :
		    dsdaolist=[]
		    iPds = 0
		    fpdsInc = 10
		    pdsID = self.sm.increment(conn, "SEQ_DP", transaction=tran, incCount=fpdsInc)
		    for ads in fpds:
			if iPds == fpdsInc:
			    pdsID = self.sm.increment(conn, "SEQ_DP", transaction=tran, incCount=fpdsInc)
			    iPds = 0
			dsdao={ "this_dataset_id": dataset_id }
			dsdao["parent_dataset_id"] = ads
			dsdao["dataset_parent_id"] = pdsID # PK of table 
			dsdaolist.append(dsdao)
		    # Do this one by one, as its sure to have duplicate in dest table
		    for adsp in dsdaolist:
			try:
			    self.dsparentin.execute(conn, adsp, transaction=tran)
			except exceptions.IntegrityError, ex:
			    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
				pass
			    else:
				raise
		# Update block parameters, file_count, block_size
		blkParams=self.blkstats.execute(conn, block_id, transaction=tran)
		blkParams['block_size']=long(blkParams['block_size'])
		self.blkstatsin.execute(conn, blkParams, transaction=tran)
	    # Delete the just inserted files
	    self.bufdeletefiles.execute(conn, flfnl, transaction=tran)
	    # All good ?. 
            tran.commit()

	except Exception, e:
	    tran.rollback()
	    self.logger.exception(e)
	    raise

	finally:
	    conn.close()
	
