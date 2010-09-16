#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""

__revision__ = "$Id: DBSBlock.py,v 1.13 2010/01/01 19:00:36 akhukhun Exp $"
__version__ = "$Revision: 1.13 $"

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils

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


    def listBlocks(self, dataset="", block_name="", site_name=""):
        """
        dataset, block_name or site_name must be passed.
        """
        return self.blocklist.execute(dataset, block_name, site_name)
    
    
    def insertBlock(self, businput):
        """
        Input dictionary has to have the following keys:
        blockname
	
        It may have:
        open_for_writing, origin_site(name), block_size,
        file_count, creation_date, create_by, last_modification_date, last_modified_by
        
        it builds the correct dictionary for dao input and executes the dao

        NEED to validate there are no extra keys in the businput
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["dataset_id"] = self.datasetid.execute((businput["block_name"]).split('#')[0], conn, True)
            businput["block_id"] =  self.sm.increment("SEQ_BK", conn, True)
            businput["last_modification_date"]=  dbsUtils().getTime()
            businput["last_modified_by"] = dbsUtils().getModifiedBy()
            if(businput.has_key("origin_site")):
                businput["origin_site"] = self.siteid.execute(businput["origin_site"], conn, True)
            if("create_by" not in businput):
                businput["create_by"] = dbsUtils().getCreateBy()
            if("creation_date" not in businput):
                businput["creation_date"] = dbsUtils().getTime()
            self.blockin.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
