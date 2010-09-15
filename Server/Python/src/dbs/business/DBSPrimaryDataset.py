#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.1 2009/10/15 19:03:51 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi):
        """
        initialize business object class.
        """
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listPrimaryDatasets(self, primdsname=""):
        """
        does not work for now
        """
        primdslist = self.daofactory(classname="PrimaryDataset.List")
        result = primdslist.execute(primdsname)
        return result

    def insertPrimaryDataset(self, primdsObject):
        """
        Input dictionary has to have the following keys:
        primarydsname, primarydstype, creationdate, createby
        it builds the correct dictionary for dao input and executes the dao
        """
        primdstplist = self.daofactory(classname="PrimaryDSType.List")
        primdsinsert = self.daofactory(classname="PrimaryDataset.Insert")
        seqmanager = self.daofactory(classname="SequenceManager")

        primdsname = primdsObject["primarydsname"]
        primdstype = primdsObject["primarydstype"]
        primdsObj = primdsObject
        primdsObj.pop("primarydstype")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            #get primary ds type id
            primarydstypeslist = primdstplist.execute(primdstype, conn, True)
            assert len(primarydstypeslist) == 1, "PrimaryDSType %s does not exist" % (primdstype)
            print primarydstypeslist
            primdstypeid = primarydstypeslist[0]["primary_ds_type_id"]
            #get primary ds id
            primdsid = seqmanager.increment("SEQ_PDS", conn, True)
            #update binds, execute & commit
            primdsObj.update({"primarydstypeid":primdstypeid, "primarydsid":primdsid})
            primdsinsert.execute(primdsObj, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
