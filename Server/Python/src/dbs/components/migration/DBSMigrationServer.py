import cherrypy
from threading import *
import logging
from logging.handlers import HTTPHandler, RotatingFileHandler
#from cherrypy import log as cplog
#from cherrypy._cplogging import LogManager

MgrLogger = cherrypy.log.error_log
#MgrLogger2 = cherrypy.log.access_log

class CheeryPyConfig():
    def __init__(self, fileName='dbsMigration.log',  maxBytes = 2000000, backupCount = 30):
        handler = RotatingFileHandler(fileName, "a", maxBytes, backupCount)
        logging.getLogger().addHandler(handler)

    def _configCherryPy(self):
        cherrypy.config.update({'server.socket_port': 16666})
        cherrypy.config.update({'log.screen' : True})
        #cherrypy.config.update({'log.access_file':'/uscms/home/yuyi/dbs3-test/logs/migration.log'})
        #cherrypy.config.update({'log.error_file':'/uscms/home/yuyi/dbs3-test/logs/migration.log'})
        #not sure about the thread pool here
        cherrypy.config.update({'server.thread_pool' :10})
    #
        cherrypy.log.error_log.setLevel(logging.INFO)
        cherrypy.log.access_log.setLevel(logging.INFO)




class DBSMigrationServer(Thread):
    
    def __init__(self, func, duration=2):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        #Thread.setDaemon()
        self.wakeUp = Condition()
        self.stopFlag = False
        self.taskFunc = func
        self.duration = duration
        try: 
            name = func.__class__.__name__
        except:
            name = func.__name__
        Thread.__init__(self)
        self.name = name
        cherrypy.engine.subscribe('start', self.start, priority = 100)
        cherrypy.engine.subscribe('stop', self.stop, priority = 100)
        
    def stop(self):
        MgrLogger.info("Stopping thread %s" %self.getName())   #YG
        #shut down all the db connections before the stop.
        #cleanup everything. 
        #The condition lock should let the running job to finish all it need to be done.
        #How long can cheerypy can wait?
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()
    
    def isStopFlagOn(self):
        # this function can be used if the work needs to be gracefully 
        # shut down by setting the several stopping point in the self.taskFunc
        return self.stopFlag
    
    def run(self):
        while not self.stopFlag:
            self.wakeUp.acquire()
            #MgrLogger.info( str(currentThread()) + ',' + self.name )
            self.taskFunc(self.isStopFlagOn)
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
from WMCore.Configuration import *
from cherrypy import HTTPError
import json, cjson
class SequencialTaskBase(object):
    
    def __init__(self, *args, **kwargs):
        self.initialize(*args, **kwargs)
        self.setCallSequence()
        
    def __call__(self, stopFlagFunc):
        #print stopFlagFunc.__class__.__name__
        #print stopFlagFunc
        for call in self._callSequence:
            if stopFlagFunc():
                return
            try:
                call()
            except Exception, ex:
                #log the excpeiotn and break. 
                #SequencialTasks are interconnected between functions  
                MgrLogger.error(str(ex))  #YG
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
        self.callSequence = [self.do_something1, self.do_something2]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")

   
class MigrationTask(SequencialTaskBase):
    
    def initialize(self, config):
        self.config = loadConfigurationFile(config)
        #self.logger = cherrypy.log.error_log
        self.sourceUrl = None
        self.migration_req_id = 0
        self.block_names = []
        self.migration_block_ids = []
        self.inserted = True
        connectUrl = None
        dbowner =  self.config.dbs.views.active.DBSMigrator.database.instances.section_('prod/global').dbowner
        connectUrl = self.config.dbs.views.active.DBSMigrator.database.instances.section_('prod/global').connectUrl
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
            
            if len(req) == 1 :
                self.sourceUrl = req[0]['migration_url']
                self.migration_req_id = req[0]['migration_request_id']
                MgrLogger.info("-"*20+ 'Migration request ID: '+ str(self.migration_req_id))
                migration_status = 1
                self.dbsMigrate.updateMigrationRequestStatus(migration_status, self.migration_req_id)
            else: return #No request found. Exit.
        except Exception, ex:
            MgrLogger.error(str(ex))
            self.sourceUrl = None
            return   # don't need to go down.
            
        #query migration_blocks table to get a list of block_names to be migrated
        # and migration_block_id list. Both lists are ordered by MIGRATION_ORDER
        try:
            blocks = self.dbsMigrate.listMigrationBlocks(self.migration_req_id)
            for b in blocks:
                self.block_names.append(b['migration_block_name'])
                self.migration_block_ids.append(b['migration_block_id'])
            MgrLogger.info("-"*20+ 'Regester ID: %s '%self.migration_req_id + 'Migration Block Names: ')
            MgrLogger.info("block_name: %s" %self.block_names)
            if not self.block_names : 
                logmessage="No migration blocks found under the migration request id %s." %(self.migration_req_id )+ \
                           "It could be the blocks in a wrong status due to privious error. Please check it."
                #set MIGRATION_STATUS = 3(failed) for MIGRATION_REQUESTS
                self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
                #raise HTTPError 409
                dbsExceptionHandler("dbsException-conflict-data", "No migration blocks found", MgrLogger.error, logmessage)
            else:
                #Update MIGRATION_STATUS for all the MIGRATION_BLOCK_IDs in the self.migration_block_id list
                #in MIGRATION_BLOCKS table to 1 (in progress)
                #set MIGRATION_STATUS = 1 and commit it immediately
                self.dbsMigrate.updateMigrationBlockStatus(migration_status=1, migration_block=self.migration_block_ids)
        except HTTPError, her:
            self.sourceUrl = None
            raise her
        except Exception, ex:
            self.sourceUrl = None
            MgrLogger.error(str(ex))
            #set MIGRATION_STATUS = 3(failed) for MIGRATION_REQUESTS
            self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
    def insertBlock(self):
        #MgrLogger.info("_"*20+"insertBlock")
        self.inserted = True
        if self.sourceUrl:
            try:
                for bName in self.block_names:
                    params={'block_name':bName}
                    data = self.dbsMigrate.callDBSService(self.sourceUrl, 'blockdump', params)
                    data = cjson.decode(data)
                    idx = self.block_names.index(bName)
                    MgrLogger.info("-"*20 +"Inserting block: %s for request id: %s" %(bName, self.migration_req_id))
                    try:
                        self.DBSBlockInsert.putBlock(data, migration=True)
                        self.dbsMigrate.updateMigrationBlockStatus(migration_status=2,
                            migration_block=self.migration_block_ids[idx])
                    except dbsException, de:
                        if "Block already exists" in de.message:
                            #the block maybe get into the destination by other means. 
                            #skip this block and continue.
                            self.dbsMigrate.updateMigrationBlockStatus(migration_status=2, 
                                migration_block=self.migration_block_ids[idx])
                        else:
                            self.dbsMigrate.updateMigrationBlockStatus(migration_status=3,
                                 migration_block=self.migration_block_ids[idx])
                            raise 
                    except Exception, ex:
                        self.dbsMigrate.updateMigrationBlockStatus(migration_status=3,
                            migration_block=self.migration_block_ids[idx])
                        raise
                    MgrLogger.info("-"*20 +"Done insert block: %s for request id: %s" %(bName,self.migration_req_id))
                self.dbsMigrate.updateMigrationRequestStatus(2, self.migration_req_id)
            except Exception, ex:
                self.inserted = False
                #handle dbsException
                if type(ex) == dbsException:
                    MgrLogger.error(ex.message + ex.serverError)
                MgrLogger.error(str(ex))
                self.dbsMigrate.updateMigrationRequestStatus(3, self.migration_req_id)
                self.dbsMigrate.updateMigrationBlockStatus(migration_status=3, migration_request=self.migration_req_id)
                return

    def cleanup(self):
        #MgrLogger.info("_"*20+"cleanup")
        #return to the initial status
        self.sourceUrl = None
        self.migration_req_id = 0
        self.block_name = []
        self.migration_block_id = []
        self.inserted = True 

if __name__ == '__main__':
    for i in range(4):
        #we need to use a simplfied config file for migration instead of the current DBS one? 
        DBSMigrationServer(MigrationTask("/uscms/home/yuyi/dbs3-test/DBS/Server/Python/control/DBSConfig.py"), 5)
    CheeryPyConfig(fileName=os.path.join('/uscms/home/yuyi/dbs3-test/logs', 'DBS333migration.log'))._configCherryPy()
    MgrLogger.info("*********** DBS Migration Server Starting. ************")
    cherrypy.quickstart()

