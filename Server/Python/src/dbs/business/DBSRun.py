#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Dataset Run table.
"""

__revision__ = "$Id: DBSRun.py,v 1.11 2010/07/09 18:23:27 yuyi Exp $"
__version__ = "$Revision: 1.11 $"

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSRun:
    """
    Site business object class
    """

    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.runlist = daofactory(classname="DatasetRun.List")

    def listRuns(self, run=-1, logical_file_name="",
                 block_name="", dataset=""):
        """
        List run known to DBS.
        """
        if( '%' in logical_file_name or '%' in block_name or '%' in dataset ):
            dbsExceptionHandler('dbsException-invalid-input', 
                                " DBSDatasetRun/listRuns. No wildcards are allowed in logical_file_name, block_name or dataset.\n.")
        conn = self.dbi.connection()
        tran = False
        try:
            ret = self.runlist.execute(conn, run, logical_file_name, block_name, dataset, tran)
            result = []
            rnum = []
            for i in ret:
                rnum.append(i['run_num'])
            result.append({'run_num' : rnum})
            return result

        finally:
            if conn:
                conn.close()
