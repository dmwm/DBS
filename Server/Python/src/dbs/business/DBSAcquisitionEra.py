#!/usr/bin/env python
"""
This module provides business object class to interact with DBSAcqusitionEra. 
"""

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSAcquisitionEra:
    """
    DBSAcqusition Era business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.acqin = daofactory(classname="AcquisitionEra.Insert")
        self.acqlst = daofactory(classname="AcquisitionEra.List")
        self.acqlst_ci = daofactory(classname="AcquisitionEra.List_CI")
        self.acqud = daofactory(classname="AcquisitionEra.UpdateEndDate")

        self.sm = daofactory(classname="SequenceManager")

    def listAcquisitionEras(self, acq=''):
        """
        Returns all acquistion eras in dbs
        """
        try:
            acq = str(acq)
        except:
            dbsExceptionHandler('dbsException-invalid-input', 'acquistion_era_name given is not valid : %s' %acq)
        conn = self.dbi.connection()
        try:
            result = self.acqlst.execute(conn, acq)
            return result
        finally:
            if conn:conn.close()

    def listAcquisitionEras_CI(self, acq=''):
        """
        Returns all acquistion eras in dbs
        """
        try:
            acq = str(acq)
        except:
            dbsExceptionHandler('dbsException-invalid-input', 'aquistion_era_name given is not valid : %s'%acq)
        conn = self.dbi.connection()
        try:
            result = self.acqlst_ci.execute(conn, acq)
            return result
        finally:
            if conn:conn.close()

    def insertAcquisitionEra(self, businput):
        """
        Input dictionary has to have the following keys:
        acquisition_era_name, creation_date, create_by, start_date, end_date.
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["acquisition_era_id"] = self.sm.increment(conn, "SEQ_AQE", tran)
            businput["acquisition_era_name"] = businput["acquisition_era_name"]
            #self.logger.warning(businput)
            self.acqin.execute(conn, businput, tran)
            tran.commit()
            tran = None
        except KeyError, ke:
            dbsExceptionHandler('dbsException-invalid-input', "Invalid input:"+ke.args[0])
        except Exception, ex:
            if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                dbsExceptionHandler('dbsException-invalid-input2', "Invalid input: acquisition_era_name already exists in DB",  serverError="%s" %ex)
            else:
                raise
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()

    def UpdateAcqEraEndDate(self, acquisition_era_name ="", end_date=0):
        """
        Input dictionary has to have the following keys:
        acquisition_era_name, end_date.
        """
        if acquisition_era_name =="" or end_date==0:
            dbsExceptionHandler('dbsException-invalid-input', "acquisition_era_name and end_date are required")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            self.acqud.execute(conn, acquisition_era_name, end_date, tran)
            if tran:tran.commit()
            tran = None
        finally:
            if tran:tran.rollback()
            if conn:conn.close()
