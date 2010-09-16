#!/usr/bin/env python
"""
This module provides business object class to interact with DBSAcqusitionEra. 
"""

__revision__ = "$Id: DBSAcquisitionEra.py,v 1.1 2009/12/23 20:08:18 afaq Exp $"
__version__ = "$Revision $"

from WMCore.DAOFactory import DAOFactory

class DBSAcquisitionEra:
    """
    DBSAcqusition Era business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        #self.acqlist = daofactory(classname="AcquisitionEra.List")
        self.acqin = daofactory(classname="AcquisitionEra.Insert")
        self.sm = daofactory(classname="SequenceManager")


    def listAcquisitionEraEras(self):
        """
        Returns all primary datasets if primdsname is not passed.
        """
        return self.acqlist.execute()


    def insertAcquisitionEra(self, businput):
        """
        Input dictionary has to have the following keys:
        primary_ds_name, primary_ds_type, creation_date, create_by
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    businput["acquisition_era_id"] = self.sm.increment("SEQ_PDS", conn, True)
	    assert businput["creation_date"]
	    assert businput["create_by"]
            self.acqin.execute(businput, conn, True)
            tran.commit()
	except IndexError:
	    self.logger.exception( "DBS Error: Index error raised")
	    #self.logger.error( "Index error raised")
	    raise 
        except Exception, ex:
                if str(ex).lower().find("unique constraint") != -1 :
                        # dataset already exists, lets fetch the ID
                        self.logger.warning("Unique constraint violation being ignored...")
                        self.logger.warning("%s" % ex)
			pass
		else:
            		tran.rollback()
            		self.logger.exception(ex)
            		raise
        finally:
            conn.close()
