#!/usr/bin/env python
"""
This module provides FileLumi.List data access object.
"""
__revision__ = "$Id: List.py,v 1.7 2010/08/05 16:08:24 yuyi Exp $"
__version__ = "$Revision: 1.7 $"


from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    FileLumi List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT FL.RUN_NUM as RUN_NUM, FL.LUMI_SECTION_NUM as LUMI_SECTION_NUM
"""

    def execute(self, conn, logical_file_name='', block_name=''):
        """
        Lists lumi section numbers with in a file or a block.
        """
        if not conn:
            raise Exception("dbs/dao/Oracle/FileLumi/List expects db connection from upper layer.")
            
        #sql = self.sql
        
        if logical_file_name:
            sql = self.sql + """ FROM %sFILE_LUMIS FL JOIN %sFILES F ON F.FILE_ID = FL.FILE_ID 
	    WHERE F.LOGICAL_FILE_NAME = :logical_file_name""" % ((self.owner,)*2)
	    binds = {'logical_file_name': logical_file_name}
        elif block_name:
            sql = self.sql + """ , F.LOGICAL_FILE_NAME as LOGICAL_FILE_NAME   
	              FROM %sFILE_LUMIS FL JOIN %sFILES F ON F.FILE_ID = FL.FILE_ID  
	              JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID  
		      WHERE B.BLOCK_NAME = :block_name"""  % ((self.owner,)*3)
            binds = {'block_name': block_name}
        else:
            raise Exception("Either logocal_file_name or block_name must be provided.")
        
	cursors = self.dbi.processData(sql, binds, conn, transaction=False, returnCursor=True)
	if len(cursors) != 1:
	    raise Exception("file lumi does not exist.")
        result = self.formatCursor(cursors[0])
        return result
