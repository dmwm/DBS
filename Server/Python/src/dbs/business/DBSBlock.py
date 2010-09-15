#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.4 2009/11/03 16:41:25 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.DAOFactory import DAOFactory

class DBSBlock:
    """
    Block business object class
    """
    def __init__(self, logger, dbi):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listBlocks(self, dataset, block = ""):
        """
        Returns all blocks in a dataset if block pattern ("can include %") 
        is not provided.
        """
        dao = self.daofactory(classname="Block.List")
        return dao.execute(dataset, block)

    def insertBlock(self, businput):
        """
        Input dictionary has to have the following keys:
        blockname, dataset(id), openforwriting, originsite(name), blocksize,
        filecount, creationdate, createby, lastmodificationdate, lastmodifiedby
        it builds the correct dictionary for dao input and executes the dao
        """
        datasetgetid = self.daofactory(classname = "Dataset.GetID")
        sitegetid = self.daofactory(classname = "Site.GetID")
        blockinsert = self.daofactory(classname = "Block.Insert")
        seqmanager = self.daofactory(classname = "SequenceManager")

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["dataset"] = datasetgetid.execute(businput["dataset"], conn, True)
            businput["originsite"] = sitegetid.execute(businput["originsite"], conn, True)
            businput["blockid"] =  seqmanager.increment("SEQ_BK", conn, True)
            blockinsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
