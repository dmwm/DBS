#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.4 2009/11/24 10:58:12 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

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

    def listPrimaryDatasets(self, primdsname=""):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        primdslist = self.daofactory(classname="PrimaryDataset.List")
        result = primdslist.execute(primdsname)
        return result

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primarydsname, primarydstype, creationdate, createby
        it builds the correct dictionary for dao input and executes the dao
        """
        primdstpgetid = self.daofactory(classname="PrimaryDSType.GetID")
        primdsinsert = self.daofactory(classname="PrimaryDataset.Insert")
        seqmanager = self.daofactory(classname="SequenceManager")

        primdsname = businput["primarydsname"]
        primdstype = businput["primarydstype"]
        #primdsObj = businput
        businput.pop("primarydstype")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            primdstypeid = primdstpgetid.execute(primdstype) 
            primdsid = seqmanager.increment("SEQ_PDS", conn, True)
            businput.update({"primarydstypeid":primdstypeid, "primarydsid":primdsid})
            primdsinsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
