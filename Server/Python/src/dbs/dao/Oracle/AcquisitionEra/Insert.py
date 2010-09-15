#!/usr/bin/env python
""" DAO Object for AcquisitionEras table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:16 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sACQUISITION_ERAS ( ACQUISITION_ERA_ID, ACQUISITION_ERA_NAME, CREATION_DATE, CREATE_BY, DESCRIPTION) VALUES (:acquisitioneraid, :acquisitioneraname, :creationdate, :createby, :description) % (self.owner) ;"""

    def getBinds_delme( self, acquisition_erasObj ):
            binds = {}
            if type(acquisition_erasObj) == type ('object'):
            	binds = {
			'acquisitioneraid' : acquisition_erasObj['acquisitioneraid'],
			'acquisitioneraname' : acquisition_erasObj['acquisitioneraname'],
			'creationdate' : acquisition_erasObj['creationdate'],
			'createby' : acquisition_erasObj['createby'],
			'description' : acquisition_erasObj['description'],
                 }

            elif type(acquisition_erasObj) == type([]):
               binds = []
               for item in acquisition_erasObj:
                   binds.append({
 	                'acquisitioneraid' : item['acquisitioneraid'],
 	                'acquisitioneraname' : item['acquisitioneraname'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'description' : item['description'],
 	                })
               return binds


    def execute( self, acquisition_erasObj, conn=None, transaction=False ):
            ##binds = self.getBinds( acquisition_erasObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


