#!/usr/bin/env python
"""
DBS Migration Service Component Module
"""
__revision__ = "$Id: DBSMigrationService.py,v 1.3 2010/08/02 20:49:36 afaq Exp $"
__version__ = "$Revision: 1.3 $"


import logging
import threading
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from dbs.components.insertbuffer.DBSInsertBufferPoller import DBSInsertBufferPoller

#['__call__', '__doc__', '__init__', '__module__', '__str__', 'config', 'handleMessage', 'initInThread', 'initialization', 'logState', 'postInitialization', 'preInitialization', 'prepareToStart', 'publishItem', 'startComponent']

class DBSMigrationService(Harness):
    def __init__(self, config):
	# call the base class
	Harness.__init__(self, config)
	self.pollTime = 1
	print "DBS Migration Service Initialization"
	

    def preInitialization(self):
					    
	# use a factory to dynamically load handlers.
	factory = WMFactory('generic')
					    
	# Add event loop to worker manager
	myThread = threading.currentThread()

	pollInterval = self.config.DBSMigrationService.pollInterval
	logging.info("Setting poll interval to %s seconds for the migration service" % pollInterval)
	myThread.workerThreadManager.addWorker(DBSMigrationServicePoller(self.config), pollInterval)
	return


	    
