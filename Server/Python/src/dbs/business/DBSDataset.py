#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.1 2009/10/21 21:15:17 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Dataset business object class
    """
    def __init__(self, logger, dbi):
        """
        initialize business object class.
        """
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listDatasets(self):
        """
        does not work for now
        """
        dslist = self.daofactory(classname="Dataset.List")
        result = dslist.execute()
        return result

