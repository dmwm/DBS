#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.1 2009/10/27 17:24:47 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSBlock:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listBlocks(self, primdsname=""):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        primdslist = self.daofactory(classname="Block.List")
        result = primdslist.execute(primdsname)
        return result

    def insertBlock(self, businput):
        """
        Input dictionary has to have the following keys:
        blockname, dataset, openforwriting, sitename, blocksize,
        filecount, creationdate, createby, lastmodificationdate, lastmodifiedby
        it builds the correct dictionary for dao input and executes the dao
        """
         = self.daofactory(classname="PrimaryDSType.List")
        primdsinsert = self.daofactory(classname="PrimaryDataset.Insert")
        seqmanager = self.daofactory(classname="SequenceManager")

        primdsname = primdsObject["primarydsname"]
        primdstype = primdsObject["primarydstype"]
        primdsObj = primdsObject
        primdsObj.pop("primarydstype")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            primarydstypeslist = primdstplist.execute(primdstype, conn, True)
            assert len(primarydstypeslist) == 1, "PrimaryDSType %s does not exist" % (primdstype)
            primdstypeid = primarydstypeslist[0]["primary_ds_type_id"]
            primdsid = seqmanager.increment("SEQ_PDS", conn, True)
            primdsObj.update({"primarydstypeid":primdstypeid, "primarydsid":primdsid})
            primdsinsert.execute(primdsObj, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
