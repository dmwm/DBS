#!/usr/bin/env python
"""
This module provides business object class to interact with various status tables, such as COMPONENT_STAUS and DBS_VERSIONS tables. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS

class DBSStatus:
    """
    Site business object class
    """

    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

	self.compstatus = daofactory(classname="ComponentStatus.List")
	self.dbsstatus = daofactory(classname="ComponentStatus.DBSStatus")

    def getComponentStatus(self):
        """
        List the status of components running with this DBS instance
        """
	try:
	    conn = self.dbi.connection()
	    ret = self.compstatus.execute(conn)
	    return ret
	except Exception, ex:
            #self.logger.exception("%s DBSStatus/getComponentStatus. %s" \
                    #%(DBSEXCEPTIONS['dbsException-2'], ex) )
	    raise ex
	finally:
		conn.close()

    def getSchemaStatus(self):
      """
	Picks up schema version information from database
      """
      try:
	conn = self.dbi.connection()
	ret = self.dbsstatus.execute(conn)
	return ret
      except Exception, ex:
        #self.logger.exception("%s DBSStatus/getSchemaStatus. %s" \
                #%(DBSEXCEPTIONS['dbsException-2'], ex) )
	raise ex
      finally:
	conn.close()
	
          
