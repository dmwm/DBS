#!/usr/bin/env python
"""
This module provides business object class to interact with various status tables, such as COMPONENT_STAUS and DBS_VERSIONS tables. 
"""

__revision__ = "$Id: DBSStatus.py,v 1.1 2010/06/14 15:09:50 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

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
	raise ex
      finally:
	conn.close()
	
          
