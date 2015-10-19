#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with DBSProcessingEra. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class DBSProcessingEra:
    """
    DBSProcessing Era business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.pein = daofactory(classname="ProcessingEra.Insert")
        self.pelst = daofactory(classname="ProcessingEra.List")
        self.sm = daofactory(classname="SequenceManager")

    def listProcessingEras(self, processing_version=''):
        """
        Returns all processing eras in dbs
        """
        conn = self.dbi.connection()
        try:
            result = self.pelst.execute(conn, processing_version)
            return result
        finally:
            if conn:
                conn.close()

    def insertProcessingEra(self, businput):
        """
        Input dictionary has to have the following keys:
        processing_version, creation_date,  create_by, description
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["processing_era_id"] = self.sm.increment(conn, "SEQ_PE", tran)
            businput["processing_version"] = businput["processing_version"]
            self.pein.execute(conn, businput, tran)
            tran.commit()
            tran = None
        except KeyError as ke:
            dbsExceptionHandler('dbsException-invalid-input',
                                "Invalid input:" + ke.args[0])
        except Exception as ex:
            if (str(ex).lower().find("unique constraint") != -1 or
                str(ex).lower().find("duplicate") != -1):
                        # already exist
                self.logger.warning("DBSProcessingEra/insertProcessingEras. " +
                                "Unique constraint violation being ignored...")
                self.logger.warning(ex)
            else:
                if tran:
                    tran.rollback()
                    tran = None
                raise
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
