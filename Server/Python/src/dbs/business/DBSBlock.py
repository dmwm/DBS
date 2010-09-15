#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.3 2009/10/30 16:41:24 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DAOFactory import DAOFactory

class DBSBlock:
    """
    Primary Dataset business object class
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
        result = dao.execute(dataset, block)
        return result

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
