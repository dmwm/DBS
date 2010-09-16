#!/usr/bin/env python
""" DAO Object for BlockSites table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/06/23 21:21:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
            self.sql = """INSERT INTO %sBLOCK_SITES ( BLOCK_SITE_ID, SITE_ID, BLOCK_ID) VALUES (:blocksiteid, (SELECT SITE_ID FROM SITES WHERE SITE_NAME=:sitename), :blockid)""" % (self.owner)

    def execute( self, conn, block_site_id="", block_id="", site_name="", transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/BlockSite/Insert expects db connection from upper layer.")
	binds={}
	binds['blocksiteid']=block_site_id
	binds['blockid']=block_id
	binds['sitename']=site_name
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


