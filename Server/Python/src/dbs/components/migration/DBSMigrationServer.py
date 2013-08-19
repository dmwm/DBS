import time
import cherrypy
import threading
import logging
from logging.handlers import HTTPHandler, RotatingFileHandler

MgrLogger = cherrypy.log.error_log
#MgrLogger = cherrypy.log

class MigrationWebMonitoring(object):
    """Exposed web-site for lemon monitoring (only exposed to localhost)"""
    def index(self):
        return 'DBS3 Migration Service'
    index.exposed = True

class DBSMigrationServer(threading.Thread):
    
    def __init__(self, func, duration=2):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        #Thread.setDaemon()

        threading.Thread.__init__(self)
        
        self.wakeUp = threading.Condition()
        #stopFlag can be used if the work needs to be gracefully 
        #shut down by setting the several stopping point in the self.taskFunc
        self.stopFlag = False
        self.taskFunc = func
        self.duration = duration
        if type(func) == type(lambda :None):
            name = func.__name__
        else:
            name = func.__class__.__name__

        self.name = name
        cherrypy.engine.subscribe('start', self.start, priority = 100)
        cherrypy.engine.subscribe('stop', self.stop, priority = 100)
        
    def stop(self):
        MgrLogger.error( time.asctime(time.gmtime()) + "Stopping thread %s" %self.getName())   #YG
        #shut down all the db connections before the stop.
        #cleanup everything. 
        #The condition lock should let the running job to finish all it need to be done.
        #How long can cheerypy can wait?
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()
    
    def run(self):
        while not self.stopFlag:
            self.wakeUp.acquire()
            #MgrLogger.info( str(currentThread()) + ',' + self.name )
            self.taskFunc(self.stopFlag)
            self.wakeUp.wait(self.duration)
            self.wakeUp.release()
         
#import logging
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException,dbsExceptionCode
from dbs.business.DBSMigrate import DBSMigrate  
from dbs.business.DBSBlockInsert import DBSBlockInsert
from cherrypy import HTTPError
import json, cjson
class SequencialTaskBase(object):
    
    def __init__(self, *args, **kwargs):
        self.initialize(*args, **kwargs)
        self.setCallSequence()
        
    def __call__(self, stopFlag):
        for call in self._callSequence:
            if stopFlag:
                return
            try:
                call()
            except Exception, ex:
                #log the excpeiotn and break. 
                #SequencialTasks are interconnected between functions  
                MgrLogger.error( time.asctime(time.gmtime()) + str(ex))  #YG
                break
            
    def initialize(self, *args, **kwargs):
        """
        Has to be defined in user code.
        """
        raise NotImplementedError("Initialize args for the member functions")
    
    def setCallSequence(self):
        """
        Has to be defined in user code.
        set the list of function call with out args on self._callSequence
        
        i.e.
        self._callSequence = [self.do_something1, self.do_something2]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")

   
class MigrationTask(SequencialTaskBase):
    
    def initialize(self, db_config):
        self.db_config = db_config
        self.sourceUrl = None
        self.migration_req_id = 0
        self.block_names = []
        self.migration_block_ids = []
        self.inserted = True
        dbowner = self.db_config.get('dbowner')
        connectUrl = self.db_config.get('connectUrl')
        dbFactory = DBFactory(MgrLogger, connectUrl, options={})
        self.dbi = dbFactory.connect()
        self.dbFormatter = DBFormatter(MgrLogger,self.dbi)
        self.dbsMigrate = DBSMigrate(MgrLogger, self.dbi, dbowner)
        self.DBSBlockInsert = DBSBlockInsert(MgrLogger, self.dbi, dbowner)
    
    def setCallSequence(self):
        self._callSequence = [self.getResource, self.insertBlock, self.cleanup]
    
    def getResource(self):
        #MgrLogger.info("_"*20+ "getResource")
        #query the MIGRATION_REQUESTS table to get a request with the smallest CREATION_DATE
        # and MIGRATION_STATUS = 0(pending)
        try:
            #set  MIGRATION_STATUS = 1(in progess) and commit it imeditaly to avoid other threads to touch it.
            req = self.dbsMigrate.listMigrationRequests(oldest=True)
            
            try:
                request =req[0]
                self.sourceUrl = request['migration_url']
                self.migration_req_id = request['migration_request_id']
                MgrLogger.error("-"*20+  time.asctime(time.gmtime()) + 'Migration request ID: '+ str(self.migration_req_id))
                migration_status = 1
                self.dbsMigrate.updateMigrationRequestStatus(migration_status, self.migration_req_id)
            except IndexError: 
                return #No request found. Exit.
        except Exception, ex:
            MgrLogger.error( time.asctime(time.gmtime()) + str(ex) )
            self.sourceUrl = None
            return   # don't need to go down.

        self.block_names = []
        self.migration_block_ids = []
        #query migration_blocks table to get a list of block_names to be migrated
        # and migration_block_id list. Both lists are ordered by MIGRATION_ORDER
        try:
            blocks = self.dbsMigrate.listMigrationBlocks(self.migration_req_id)
            for b in blocks:
                self.block_names.append(b['migration_block_name'])
                self.migration_block_ids.append(b['migration_block_id'])
            if not self.block_names : 
                logmessage="No migration blocks found under the migration request id %s." %(self.migration_req_id )+ \
                           "DBS Operator please check it."
                #set MIGRATION_STATUS = 3(failed) for MIGRATION_REQUESTS
                self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
                self.sourceUrl = None
                MgrLogger.error( time.asctime(time.gmtime()) + logmessage )
            else:
                #Update MIGRATION_STATUS for all the MIGRATION_BLOCK_IDs in the self.migration_block_id list
                #in MIGRATION_BLOCKS table to 1 (in progress)
                #set MIGRATION_STATUS = 1 and commit it immediately
                #MgrLogger.info("-"*20+ 'Regester ID: %s '%self.migration_req_id + 'Migration Block Names: ')
                #MgrLogger.info("block_name: %s" %self.block_names)
                self.dbsMigrate.updateMigrationBlockStatus(migration_status=1, migration_block=self.migration_block_ids)
        except Exception, ex:
            self.sourceUrl = None
            MgrLogger.error( time.asctime(time.gmtime()) + str(ex) )
            #set MIGRATION_STATUS = 3(failed) for MIGRATION_REQUESTS
            self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
    
    def insertBlock(self):
        #MgrLogger.info("_"*20+"insertBlock")
        self.inserted = True
        if self.sourceUrl:
            try:
                for idx, bName in enumerate(self.block_names):
                    params={'block_name':bName}
                    data = self.dbsMigrate.callDBSService(self.sourceUrl, 'blockdump', params)
                    data = cjson.decode(data)
                    migration_status = 0
                    #idx = self.block_names.index(bName)
                    MgrLogger.error("-"*20 + time.asctime(time.gmtime()) + "Inserting block: %s for request id: %s" %(bName, self.migration_req_id))
                    try:
                        self.DBSBlockInsert.putBlock(data, migration=True)
                        migration_status = 2
                    except dbsException, de:
                        if "Block %s already exists" % (bName) in de.message:
                            #the block maybe get into the destination by other means. 
                            #skip this block and continue.
                            migration_status = 2
                        else:
                            raise 
                    finally:
                        if  migration_status == 2:
                            self.dbsMigrate.updateMigrationBlockStatus(migration_status=2,
                                migration_block=self.migration_block_ids[idx])
                    MgrLogger.error("-"*20 + time.asctime(time.gmtime()) + "Done insert block: %s for request id: %s" %(bName,self.migration_req_id))
                self.dbsMigrate.updateMigrationRequestStatus(2, self.migration_req_id)
            except Exception, ex:
                self.inserted = False
                #handle dbsException
                if type(ex) == dbsException:
                    MgrLogger.error( time.asctime(time.gmtime()) + ex.message + ex.serverError )
                MgrLogger.error(time.asctime(time.gmtime()) + str(ex))
                self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
                self.dbsMigrate.updateMigrationBlockStatus(migration_status=3, migration_request=self.migration_req_id)
                return

    def cleanup(self):
        #MgrLogger.error("_"*20+"cleanup")
        #return to the initial status
        self.sourceUrl = None
        self.migration_req_id = 0
        self.block_names = []
        self.migration_block_id = []
        self.inserted = True
