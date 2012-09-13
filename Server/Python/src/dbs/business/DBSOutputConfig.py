#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with OutputConfig. 
"""

__revision__ = "$Id: DBSOutputConfig.py,v 1.15 2010/08/19 15:22:18 yuyi Exp $"
__version__ = "$Revision: 1.15 $"

from WMCore.DAOFactory import DAOFactory
from sqlalchemy import exceptions
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler


class DBSOutputConfig:
    """
    Output Config business object class
    """
    def __init__(self, logger, dbi, owner):

        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner
        
        self.outputmoduleconfiglist = daofactory(
                                    classname='OutputModuleConfig.List')

        self.sm = daofactory(classname="SequenceManager")

        self.appid = daofactory(classname='ApplicationExecutable.GetID')
        self.verid = daofactory(classname='ReleaseVersion.GetID')
        self.hashid = daofactory(classname='ParameterSetHashe.GetID')
        self.outmodin = daofactory(classname='OutputModuleConfig.Insert')
        
    def listOutputConfigs(self, dataset="", logical_file_name="", 
                          release_version="", pset_hash="", app_name="",
                          output_module_label="", block_id=0, global_tag=''):
        if '*' in logical_file_name or '%' in logical_file_name:
            dbsExceptionHandler('dbsException-invalid-input', "Fully specified logical_file_name is required. No wildcards are allowed." )
        conn = self.dbi.connection()        
        try:
            result = self.outputmoduleconfiglist.execute(conn, dataset,
                                                   logical_file_name,
                                                   app_name,
                                                   release_version,
                                                   pset_hash,
                                                   output_module_label, block_id, global_tag)
            return result
        finally:
            if conn: 
                conn.close()
    
    def insertOutputConfig(self, businput):
        """
        Method to insert the Output Config.
        app_name, release_version, pset_hash, global_tag and output_module_label are
        required.
        args:
            businput(dic): input dictionary. 

        Updated Oct 12, 2011    
        """
        if not (businput.has_key("app_name")  and businput.has_key("release_version")\
            and businput.has_key("pset_hash") and businput.has_key("output_module_label")
            and businput.has_key("global_tag")):
            dbsExceptionHandler('dbsException-invalid-input', "business/DBSOutputConfig/insertOutputConfig require:\
                app_name, release_version, pset_hash, output_module_label and global_tag")

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            # Proceed with o/p module insertion
            businput['scenario'] = businput.get("scenario", None)
            businput['pname'] = businput.get("pname", None)
            self.outmodin.execute(conn, businput, tran)
            tran.commit()
            tran = None
        except exceptions.IntegrityError, ex:
            if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                #if the validation is due to a unique constrain break in OUTPUT_MODULE_CONFIGS
                if str(ex).find("TUC_OMC_1") != -1: pass
                #otherwise, try again
                else:
                    try:
                        self.outmodin.execute(conn, businput, tran)
                        tran.commit()
                        tran =  None
                    except exceptions.IntegrityError, ex1:
                        if str(ex1).find("unique constraint") != -1 and str(ex1).find("TUC_OMC_1") != -1: pass
                    except Exception, e1:
                        if tran:
                            tran.rollback()
                            tran = None
                        raise
            else:
                raise
        except Exception, e:
            if tran:
                tran.rollback()
            raise
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
