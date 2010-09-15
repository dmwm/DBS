#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.3 2009/10/27 17:24:47 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DAOFactory import DAOFactory

class DBSDataset:
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

    def listDatasets(self, primdsname="", procdsname="", datatiername=""):
        """
        lists all datasets if none of the parameters are given.
        each parameter can include % character.
        """
        datasetlist = self.daofactory(classname="Dataset.List")
        result = datasetlist.execute(primdsname, procdsname, datatiername)
        return result

