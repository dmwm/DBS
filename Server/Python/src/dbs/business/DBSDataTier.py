#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with datatiers table. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class DBSDataTier:
    """
    DataTier business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.sm = daofactory(classname="SequenceManager")
        self.dataTier = daofactory(classname="DataTier.List")
        self.dtin = daofactory(classname="DataTier.Insert")

    def listDataTiers(self, data_tier_name=""):
        """
        List data tier(s)
        """
        if not isinstance(data_tier_name, str) :
            dbsExceptionHandler('dbsException-invalid-input',
                                'data_tier_name given is not valid : %s' % data_tier_name)
        else:
            try:
                data_tier_name = str(data_tier_name)
            except:
                dbsExceptionHandler('dbsException-invalid-input',
                                    'data_tier_name given is not valid : %s' % data_tier_name)
        conn = self.dbi.connection()
        try:
            result = self.dataTier.execute(conn, data_tier_name.upper())
            return result
        finally:
            if conn:
                conn.close()

    def insertDataTier(self, businput):
        """
        Input dictionary has to have the following keys:
        data_tier_name, creation_date, create_by
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["data_tier_id"] = self.sm.increment(conn, "SEQ_DT" )
            businput["data_tier_name"] = businput["data_tier_name"].upper()
            self.dtin.execute(conn, businput, tran)
            tran.commit()
            tran = None
        except KeyError as ke:
            dbsExceptionHandler('dbsException-invalid-input', "Invalid input:"+ke.args[0])
        except Exception as ex:
            if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                # already exist
                self.logger.warning("Unique constraint violation being ignored...")
                self.logger.warning("%s" % ex)
            else:
                if tran:
                    tran.rollback()
                raise
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
