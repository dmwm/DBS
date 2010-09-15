#!/usr/bin/env python
"""
This module provides business object class to interact with DBSProcessingEra. 
"""

__revision__ = "$Id: DBSProcessingEra.py,v 1.1 2009/12/28 16:55:29 afaq Exp $"
__version__ = "$Revision $"

from WMCore.DAOFactory import DAOFactory

class DBSProcessingEra:
    """
    DBSProcessing Era business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        #self.pelist = daofactory(classname="ProcessingEra.List")
        self.pein = daofactory(classname="ProcessingEra.Insert")
        self.sm = daofactory(classname="SequenceManager")


    def listProcessingEras(self):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        return self.pelist.execute()


    def insertProcessingEra(self, businput):
        """
        Input dictionary has to have the following keys:
        processing_version, creation_date, create_by, description
        it builds the correct dictionary for dao input and executes the dao
        """
	import pdb
	pdb.set_trace()
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    businput["processing_era_id"] = self.sm.increment("SEQ_PE", conn, True)
	    assert businput["processing_version"]
	    assert businput["description"]
	    assert businput["creation_date"]
	    assert businput["create_by"]
            self.pein.execute(businput, conn, True)
            tran.commit()
        except Exception, ex:
                if str(ex).lower().find("unique constraint") != -1 :
                        # already exist
                        self.logger.warning("Unique constraint violation being ignored...")
                        self.logger.warning("%s" % ex)
			pass
		else:
            		tran.rollback()
            		self.logger.exception(ex)
            		raise
        finally:
            conn.close()
