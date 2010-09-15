#!/usr/bin/env python
"""
This module provides FileParentBlock.List data access object.

Given the ID of a File, returns a LIST of the dicts containing IDs 
[{block_id, dataset_id},....] of the Parent BLOCK of the 
Block containing THIS file.
"""
__revision__ = "$Id: List.py,v 1.4 2010/01/19 19:44:38 afaq Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = ("","%s." % owner)[bool(owner)]
        self.sql = """SELECT B.BLOCK_ID as BLOCK_ID, B.DATASET_ID as DATASET_ID FROM %sBLOCKS B JOIN %sFILES FL ON FL.BLOCK_ID=B.BLOCK_ID LEFT OUTER JOIN %sFILE_PARENTS FP ON FP.PARENT_FILE_ID = FL.FILE_ID WHERE FP.THIS_FILE_ID IN ( """% ((self.owner,)*3)

    def execute(self, file_id_list, conn=None, transaction=False):
	"""
	file_id_list : file_id_list 
	"""
	sql=self.sql
	binds={}
	if file_id_list:
	    count=0
	    for an_id in file_id_list:
		if count > 0: sql += ", "
		sql += ":file_id_%s" %count
		binds.update({"file_id_%s" %count : an_id})
		count+=1
	    sql += ")"
	else :
	    raise Exception("this_file_id not provided")
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	return plist
