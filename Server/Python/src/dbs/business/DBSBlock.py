#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.8 2009/11/30 09:52:31 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.DAOFactory import DAOFactory

class DBSBlock:
    """
    Block business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.blocklist = daofactory(classname="Block.List")
        self.sm = daofactory(classname = "SequenceManager")
        self.datasetid = daofactory(classname = "Dataset.GetID")
        self.siteid = daofactory(classname = "Site.GetID")
        self.blockin = daofactory(classname = "Block.Insert")


    def listBlocks(self, dataset="", block=""):
        """
        either dataset or block must be provided.
        """
        return self.blocklist.execute(dataset=dataset, block=block)
    
    
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
            businput["dataset"] = self.datasetid.execute(businput["dataset"], conn, True)
            businput["originsite"] = self.siteid.execute(businput["originsite"], conn, True)
            businput["blockid"] =  self.sm.increment("SEQ_BK", conn, True)
            self.blockin.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
