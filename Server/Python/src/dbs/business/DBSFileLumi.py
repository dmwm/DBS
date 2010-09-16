#!/usr/bin/env python
"""
This module provides business object class to interact with FileLumi. 
"""

__revision__ = "$Id: DBSFileLumi.py,v 1.3 2010/03/08 23:12:34 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DAOFactory import DAOFactory

class DBSFileLumi:
    """
    DatasetParent business object class
    """
    def __init__(self, logger, dbi, owner):
        """
        initialize business object class.
        """
        daofactory = DAOFactory(package='dbs.dao', logger=logger, 
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.filelumilist = daofactory(classname="FileLumi.List")

    def listFileLumis(self, logical_file_name="", block_name=""): 
        """
        optional parameter: logical_file_name, block_name
        returns: logical_file_name, file_lumi_id, run_num, lumi_section_num
        """
	conn=self.dbi.connection()
        result=self.filelumilist.execute(conn, logical_file_name, block_name)
	return result
	conn.close()
