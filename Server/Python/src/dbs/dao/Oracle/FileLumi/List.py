#!/usr/bin/env python
"""
This module provides FileLumi.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/01/01 18:58:34 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"


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
        self.owner = ("", "%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT FL.FILE_LUMI_ID, FL.RUN_NUM, FL.LUMI_SECTION_NUM,
       F.LOGICAL_FILE_NAME
FROM %sFILE_LUMIS FL
JOIN %sFILES F ON F.FILE_ID = FL.FILE_ID
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
""" % ((self.owner,)*3)

    def execute(self, logical_file_name='', block_name='', conn=None):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        
        if logical_file_name:
            sql+="WHERE F.LOGICAL_FILE_NAME = :logical_file_name"
            binds = {'logical_file_name': logical_file_name}
        elif block_name:
            sql+="WHERE B.BLOCK_NAME = :block_name"
            binds = {'block_name': block_name}
        else:
            raise Exception("Either logocal_file_name or block_name must be provided.")
            
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
