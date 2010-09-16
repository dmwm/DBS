#!/usr/bin/env python
"""
This module provides business object class to interact with DatasetParent. 
"""

__revision__ = "$Id: DBSDatasetParent.py,v 1.3 2010/03/25 17:06:00 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DAOFactory import DAOFactory

class DBSDatasetParent:
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
        
        self.datasetparentlist = daofactory(classname="DatasetParent.List")

    def listDatasetParents(self, dataset):
        """
        takes required dataset parameter
        returns only parent dataset name
        """
	try:
	    conn = self.dbi.connection()
	    result=self.datasetparentlist.execute(conn, dataset)
	    conn.close()
	    return result
        except Exception, ex:
	    raise ex
        finally:
	    conn.close()
 
