#!/usr/bin/env python
"""
This module provides business object class to interact with FileParent. 
"""

__revision__ = "$Id: DBSFileParent.py,v 1.5 2010/03/25 17:06:00 afaq Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.DAOFactory import DAOFactory

class DBSFileParent:
    """
    DatasetParent business object class
    """
    def __init__(self, logger, dbi, owner):
        """
        initialize business object class.
        """
        daofactory = DAOFactory(package='dbs.dao', logger=logger, 
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.fileparentlist = daofactory(classname="FileParent.List")

    def listFileParents(self, logical_file_name): 
        """
        required parameter: logical_file_name
        returns: logical_file_name, parent_logical_file_name, parent_file_id
        """
	try:
	    conn=self.dbi.connection()
	    if not logical_file_name:
		raise Exception("logical_file_name is required for listFileParents api")
	    result= self.fileparentlist.execute(conn,logical_file_name)
	    conn.close()
	    return result
        except Exception, ex:
	    raise ex
	finally:
	    conn.close()

