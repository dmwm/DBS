#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSMigrateModel.py,v 1.1 2010/04/22 07:47:39 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import re
import cjson
import time
import threading
import Queue
import traceback

from cherrypy import request, response, HTTPError
from WMCore.WebTools.RESTModel import RESTModel
from dbs.utils.dbsUtils import dbsUtils


from dbs.business.DBSMigrate import DBSMigrate

	    		

class DBSMigrateModel(RESTModel):
    """
    DBS3 Server API Documentation 
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """
        RESTModel.__init__(self, config)
	
        self.addService('POST', 'start', self.start)
        self.addService('GET', 'status', self.status)
	self.dbsMigrate = DBSMigrate(self.logger, self.dbi, config.dbowner)

	self.q = Queue.Queue()
	#Need to subscribe to cherrypy.engine
	#http://tools.cherrypy.org/wiki/BackgroundTaskQueue
	for i in xrange(config.nthreads):
	    threading.Thread(target=self.run).start()
	

    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        method that adds services to the DBS rest model
        """
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': validation,
                                         'version': version}

    def start(self):
	body = request.body.read()
	indata = cjson.decode(body)
	self.dbsMigrate.insertMigrationRequest(indata)
	self.q.put(indata)
	self.logger.debug("REQUEST: %s" % indata)
	return "Migration request was successfully submitted."

    def status(self, dataset):
        dataset = dataset.replace("*", "%")
	return self.dbsMigrate.listMigrationRequests(dataset)

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
