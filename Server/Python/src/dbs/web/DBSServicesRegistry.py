#!/usr/bin/env python
"""
DBS Service Registry Rest Model module
"""
import traceback
from cherrypy import request
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
import cjson
from sqlalchemy import exceptions

class DBSServicesRegistry(RESTModel):
    """
    Service Registry REST Model Class
    """

    def __init__(self, config):
        """
        All parameters are provided through Config module
        """
        RESTModel.__init__(self, config)
    
        self.methods = {'GET':{}, 'POST':{}}
        self.addMethod('GET', 'services', self.getServices)
        self.addMethod('POST', 'services', self.addService)
        #self.dbsSrvcReg = DBSServicesRegistry(self.logger, self.dbi,
        #                                      config.dbowner)
        daofactory = DAOFactory(package='dbs.dao', logger=self.logger,
                                dbinterface=self.dbi, owner=config.dbowner)
        self.servicesadd = daofactory(classname="Service.Insert")
        self.serviceslist = daofactory(classname="Service.List")
        self.servicesupdate = daofactory(classname="Service.Update")
        self.sm = daofactory(classname = "SequenceManager")
        
    def getServices(self):
        """
        Simple method that returs list of all know DBS instances, instances known to this registry
        """
        try:
            conn = self.dbi.connection()
            result = self.serviceslist.execute(conn)
            return result
        except Exception as ex:
            msg = (("%s DBSServicesRegistry/getServices." + 
                    " %s\n. Exception trace: \n %s") %
                   (DBSEXCEPTIONS['dbsException-3'], ex,
                    traceback.format_exc()))
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )
        finally:
            conn.close()

    def addService(self):
        """
        Add a service to service registry
        """

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            
            body = request.body.read()
            service = cjson.decode(body)
            addthis = {}
            addthis['service_id'] = self.sm.increment(conn, "SEQ_RS", tran)
            addthis['name'] = service.get('NAME', '')
            if addthis['name'] == '':
                msg = (("%s DBSServicesRegistry/addServices." +
                        " Service Must be Named\n") %
                       DBSEXCEPTIONS['dbsException-3'])
                raise Exception("dbsException-3", msg)
            addthis['type'] = service.get('TYPE', 'GENERIC')
            addthis['location'] = service.get('LOCATION', 'HYPERSPACE')
            addthis['status'] = service.get('STATUS', 'UNKNOWN')
            addthis['admin'] = service.get('ADMIN', 'UNADMINISTRATED')
            addthis['uri'] = service.get('URI', '')
            if addthis['uri'] == '':
                msg = (("%s DBSServicesRegistry/addServices." + 
                        " Service URI must be provided.\n") %
                       DBSEXCEPTIONS['dbsException-3'])
                self.logger.exception(msg)
                raise Exception("dbsException-3", msg)
            addthis['db'] = service.get('DB', 'NO_DATABASE')
            addthis['version'] = service.get('VERSION', 'UNKNOWN' )
            addthis['last_contact'] = dbsUtils().getTime()
            addthis['comments'] = service.get('COMMENTS', 'NO COMMENTS')
            addthis['alias'] = service.get('ALIAS', 'No Alias')
            self.servicesadd.execute(conn, addthis, tran)
            tran.commit()
        except exceptions.IntegrityError as ex:
            if (str(ex).find("unique constraint") != -1 or
                str(ex).lower().find("duplicate") != -1) :
                #Update the service instead
                try:
                    self.servicesupdate.execute(conn, addthis, tran)
                    tran.commit()
                except Exception as ex:
                    msg = (("%s DBSServiceRegistry/addServices." + 
                            " %s\n. Exception trace: \n %s") %
                           (DBSEXCEPTIONS['dbsException-3'], ex,
                            traceback.format_exc()))
                    self.logger.exception(msg ) 
                    raise Exception ("dbsException-3", msg )
        except Exception as ex:
            tran.rollback()
            msg = (("%s DBSServiceRegistry/addServices." + 
                    " %s\n. Exception trace: \n %s") %
                   (DBSEXCEPTIONS['dbsException-3'], ex,
                    traceback.format_exc()))
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )
        finally:
            conn.close() 
