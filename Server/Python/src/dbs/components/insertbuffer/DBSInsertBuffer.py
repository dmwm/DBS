#!/usr/bin/env python
"""
DBS Insert Buffer Component Module
"""
__revision__ = "$Id: DBSInsertBuffer.py,v 1.2 2010/06/10 21:46:48 afaq Exp $"
__version__ = "$Revision: 1.2 $"


"""
Polls FILE_BUFFER table and post entries to FILES (and related) tables
"""

import logging
import threading
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from dbs.components.insertbuffer.DBSInsertBufferPoller import DBSInsertBufferPoller

#['__call__', '__doc__', '__init__', '__module__', '__str__', 'config', 'handleMessage', 'initInThread', 'initialization', 'logState', 'postInitialization', 'preInitialization', 'prepareToStart', 'publishItem', 'startComponent']

class DBSInsertBuffer(Harness):
    def __init__(self, config):
	# call the base class
	Harness.__init__(self, config)
	self.pollTime = 1
	print "DBSUpload.__init__"
	

    def preInitialization(self):
	print "DBSUpload.preInitialization"
					    
	# use a factory to dynamically load handlers.
	factory = WMFactory('generic')
					    
	# Add event loop to worker manager
	myThread = threading.currentThread()

	pollInterval = self.config.DBSInsertBuffer.pollInterval
	logging.info("Setting poll interval to %s seconds" % pollInterval)
	myThread.workerThreadManager.addWorker(DBSInsertBufferPoller(self.config), pollInterval)
	return


	    
