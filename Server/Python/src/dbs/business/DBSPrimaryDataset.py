#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.5 2009/11/27 09:55:02 akhukhun Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.PrimaryDatasetList = self.daofactory(classname="PrimaryDataset.List")
        
        self.SequenceManager = self.daofactory(classname="SequenceManager")
        self.PrimaryDSTypeGetID = self.daofactory(classname="PrimaryDSType.GetID")
        self.PrimaryDatasetInsert = self.daofactory(classname="PrimaryDataset.Insert")

    def listPrimaryDatasets(self, primdsname=""):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        return self.PrimaryDatasetList.execute(primdsname)

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primarydsname, primarydstype, creationdate, createby
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["primarydstype"] = self.PrimaryDSTypeGetID.execute(businput["primarydstype"]) 
            businput["primarydsid"] = self.SequenceManager.increment("SEQ_PDS", conn, True)
            self.PrimaryDatasetInsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
