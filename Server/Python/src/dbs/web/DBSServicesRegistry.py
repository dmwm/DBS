#!/usr/bin/env python
"""
DBS Service Registry Rest Model module
"""

__revision__ = "$Id: DBSServicesRegistry.py,v 1.1 2010/08/02 20:49:37 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from cherrypy import request, response, HTTPError
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from cherrypy import server
from dbs.utils.dbsUtils import dbsUtils
import cjson
from sqlalchemy import exceptions

class DBSServicesRegistry(RESTModel):
    """
    Service Registry REST Model Class
    """

    def __init__(self, config):
        """
        All parameters are provided through Config module
        """
        RESTModel.__init__(self, config)
    
        self.methods = {'GET':{}, 'POST':{}}
	self.addMethod('GET', 'services', self.getServices)
	self.addMethod('POST', 'services', self.addService)
	#self.dbsSrvcReg = DBSServicesRegistry(self.logger, self.dbi, config.dbowner)
	daofactory = DAOFactory(package='dbs.dao', logger=self.logger, dbinterface=self.dbi, owner=config.dbowner)
	self.servicesadd = daofactory(classname="Service.Insert")
	self.serviceslist = daofactory(classname="Service.List")
	self.servicesupdate = daofactory(classname="Service.Update")
	self.sm = daofactory(classname = "SequenceManager")
	
    def getServices(self):
        """
	Simple method that returs list of all know DBS instances, instances known to this registry
        """
	try:
	    conn=self.dbi.connection()
	    result= self.serviceslist.execute(conn)
	    return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

    def addService(self):
	"""
	Add a service to service registry
	"""

	conn = self.dbi.connection()
	tran = conn.begin()
	try:
	    
	    body = request.body.read()
            service = cjson.decode(body)
	    addthis={}
	    addthis['service_id'] = self.sm.increment(conn, "SEQ_RS", tran)
	    addthis['name'] = service.get('NAME', '')
	    if addthis['name'] == '':
		raise Exception("Service Must be Named")
	    addthis['type'] = service.get('TYPE', 'GENERIC')
	    addthis['location'] = service.get('LOCATION', 'HYPERSPACE')
	    addthis['status'] = service.get('STATUS', 'UNKNOWN')
	    addthis['admin'] = service.get('ADMIN', 'UNADMINISTRATED')
	    addthis['uri'] = service.get('URI','')
	    if addthis['uri'] == '':
		raise Exception("Service URI must be provided")
	    addthis['db'] = service.get('DB', 'NO_DATABASE')
	    addthis['version'] = service.get('VERSION','UNKNOWN' )
	    addthis['last_contact'] = dbsUtils().getTime()
	    addthis['comments'] = service.get('COMMENTS', 'NO COMMENTS')
	    addthis['alias'] = service.get('ALIAS', 'No Alias')
	    self.servicesadd.execute(conn, addthis, tran)
	    tran.commit()
	except exceptions.IntegrityError, ex:
	    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		#Update the service instead
		try:
		    self.servicesupdate.execute(conn, addthis, tran)
		    tran.commit()
		except Exception, ex:
		    raise
	except Exception, ex:
	    tran.rollback()
	    raise ex
	finally:
	    conn.close() 

