#!/usr/bin/env python
"""
This module provides business object class to interact with primary_ds_types table. 
"""

__revision__ = "$Id: DBSDataType.py,v 1.2 2010/03/19 15:04:47 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory

class DBSDataType:
    """
    Primary dataset type business object class
    """
    def __init__(self, logger, dbi, owner):
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi
	self.owner = owner

	self.dataType = daofactory(classname="PrimaryDSType.List")

    def listDataType(self, dataType="", dataset=""):
	"""
	List data-type/primary-ds-type 
	"""
	try:
	    conn = self.dbi.connection()
	    if dataset and dataType:
		raise Exception("DBSDataType can be only query by a data type or by a dataset, not both.")
	    else:
		result=self.dataType.execute(conn, dataType, dataset)
	    return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

    

