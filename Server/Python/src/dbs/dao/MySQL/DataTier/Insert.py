#!/usr/bin/env python
""" DAO Object for DataTiers table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/02/05 21:00:41 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner
	    
            self.sql = """INSERT INTO %sDATA_TIERS ( DATA_TIER_ID, DATA_TIER_NAME, CREATION_DATE, CREATE_BY) VALUES (:datatierid, :datatiername, :creationdate, :createby)""" % (self.owner)

    def getBinds_delme( self, data_tiersObj ):
            binds = {}
            if type(data_tiersObj) == type ('object'):
            	binds = {
			'datatierid' : data_tiersObj['datatierid'],
			'datatiername' : data_tiersObj['datatiername'],
			'creationdate' : data_tiersObj['creationdate'],
			'createby' : data_tiersObj['createby'],
                 }

            elif type(data_tiersObj) == type([]):
               binds = []
               for item in data_tiersObj:
                   binds.append({
 	                'datatierid' : item['datatierid'],
 	                'datatiername' : item['datatiername'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                })
               return binds


    def execute( self, data_tiersObj, conn=None, transaction=False ):
            ##binds = self.getBinds( data_tiersObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


