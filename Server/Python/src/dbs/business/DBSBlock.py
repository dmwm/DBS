#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.2 2009/10/28 09:53:24 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

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
        datasetgetid = self.daofactory(classname="Dataset.GetID")
        sitegetid = self.daofactory(classname="Site.GetID")
        blockinsert = self.daofactory(classname="Block.Insert")
        seqmanager = self.daofactory(classname="SequenceManager")

        dataset = businput["dataset"]
        businput.pop("dataset")
        sitename = businput["sitename"]
        businput.pop("sitename")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            datasetid = datasetgetid.execute(dataset, conn, True)
            siteid = sitegetid.execute(sitename, conn, True)
            blockid = seqmanager.increment("SEQ_BK", conn, True)
            businput.update({"datasetid":datasetid,
                             "originsite":siteid,
                             "blockid":blockid})
            blockinsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
