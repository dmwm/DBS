#!/usr/bin/env python
""" DAO Object for BlockSites table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """INSERT INTO %sBLOCK_SITES ( BLOCK_SITE_ID, SITE_ID, BLOCK_ID) VALUES (:blocksiteid, (SELECT SITE_ID FROM SITES WHERE SITE_NAME=:sitename), :blockid)""" % (self.owner)

    def execute( self, conn, block_site_id="", block_id="", site_name="", transaction=False ):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/BlockSite/Insert. Expects db connection from upper layer.")
            
	binds={}
	binds['blocksiteid']=block_site_id
	binds['blockid']=block_id
	binds['sitename']=site_name
	result = self.dbi.processData(self.sql, binds, conn, transaction)
        
	return


