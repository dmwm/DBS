#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.7 2009/12/16 12:04:41 akhukhun Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.primdslist = daofactory(classname="PrimaryDataset.List")
        self.sm = daofactory(classname="SequenceManager")
        self.primdstypeid = daofactory(classname="PrimaryDSType.GetID")
        self.primdsin = daofactory(classname="PrimaryDataset.Insert")


    def listPrimaryDatasets(self, primdsname=""):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        conn = self.dbi.connection()
        result = self.primdslist.execute(primdsname, conn=conn)
        conn.close()
        return result


    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primarydsname, primarydstype, creationdate, createby
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["primarydstype"] = self.primdstypeid.execute(businput["primarydstype"]) 
            businput["primarydsid"] = self.sm.increment("SEQ_PDS", conn, True)
            self.primdsin.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
