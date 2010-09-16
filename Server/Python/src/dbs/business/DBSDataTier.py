#!/usr/bin/env python
"""
This module provides business object class to interact with datatiers table. 
"""

__revision__ = "$Id: DBSDataTier.py,v 1.1 2010/04/21 19:50:01 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSDataTier:
    """
    DataTier business object class
    """
    def __init__(self, logger, dbi, owner):
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi
	self.owner = owner

	self.dataTier = daofactory(classname="DataTier.List")

    def listDataTiers(self, data_tier_name=""):
	"""
	List data tier(s)
	"""
	try:
	    conn = self.dbi.connection()
	    result = self.dataTier.execute(conn, data_tier_name)
	    return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

