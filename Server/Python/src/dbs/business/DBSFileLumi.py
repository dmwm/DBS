#!/usr/bin/env python
"""
This module provides business object class to interact with FileLumi. 
"""

__revision__ = "$Id: DBSFileLumi.py,v 1.5 2010/03/25 17:06:00 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
	try:
	    conn=self.dbi.connection()
	    result=self.filelumilist.execute(conn, logical_file_name, block_name)
	    conn.close()
	    return result
        except Exception, ex:
	    raise ex
	finally:
	    conn.close()
    
