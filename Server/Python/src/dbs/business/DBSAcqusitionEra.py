#!/usr/bin/env python
"""
This module provides business object class to interact with DBSAcqusitionEra. 
"""

__revision__ = "$Id: DBSAcqusitionEra.py,v 1.1 2009/12/23 17:51:00 afaq Exp $"
__version__ = "$Revision $"

from WMCore.DAOFactory import DAOFactory

class DBSAcqusitionEra:
    """
    DBSAcqusition Era business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.acqlist = daofactory(classname="DBSAcqusitionEra.List")
        self.acqin = daofactory(classname="DBSAcqusitionEra.Insert")
        self.sm = daofactory(classname="SequenceManager")


    def listAcqusitionEras(self):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        return self.acqlist.execute()


    def insertAcqusitionEra(self, businput):
        """
        Input dictionary has to have the following keys:
        primary_ds_name, primary_ds_type, creation_date, create_by
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    businput["acqusition_era_id"] = self.sm.increment("SEQ_PDS", conn, True)
            self.acqin.execute(businput, conn, True)
            tran.commit()
	except IndexError:
	    self.logger.exception( "DBS Error: Index error raised")
	    #self.logger.error( "Index error raised")
	    raise 
        except Exception, e:
                if str(ex).lower().find("unique constraint") != -1 :
                        # dataset already exists, lets fetch the ID
                        self.logger.warning("Unique constraint violation being ignored...")
                        self.logger.warning("%s" % ex)
			pass
		else:
            		tran.rollback()
            		self.logger.exception(e)
            		raise
        finally:
            conn.close()
