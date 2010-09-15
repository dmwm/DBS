#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.7 2009/11/27 09:55:02 akhukhun Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.DAOFactory import DAOFactory

class DBSBlock:
    """
    Block business object class
    """
    def __init__(self, logger, dbi, owner):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.BlockList = self.daofactory(classname="Block.List")

        self.SequenceManager = self.daofactory(classname = "SequenceManager")
        self.DatasetGetID = self.daofactory(classname = "Dataset.GetID")
        self.SiteGetID = self.daofactory(classname = "Site.GetID")
        self.BlockInsert = self.daofactory(classname = "Block.Insert")



    def listBlocks(self, dataset="", block=""):
        """
        Returns all blocks in a dataset if block pattern ("can include %") 
        is not provided.
        """
        return self.BlockList.execute(dataset=dataset, block=block)

    def insertBlock(self, businput):
        """
        Input dictionary has to have the following keys:
        blockname, dataset(id), openforwriting, originsite(name), blocksize,
        filecount, creationdate, createby, lastmodificationdate, lastmodifiedby
        it builds the correct dictionary for dao input and executes the dao
        """

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["dataset"] = self.DatasetGetID.execute(businput["dataset"], conn, True)
            businput["originsite"] = self.SiteGetID.execute(businput["originsite"], conn, True)
            businput["blockid"] =  self.SequenceManager.increment("SEQ_BK", conn, True)
            self.BlockInsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
