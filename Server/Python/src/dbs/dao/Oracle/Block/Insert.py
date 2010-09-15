# DAO Object for Block table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:23 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO BLOCKS(BLOCK_ID, BLOCK_NAME, DATASET_ID, OPEN_FOR_WRITING, ORIGIN_SITE, BLOCK_SIZE, FILE_COUNT, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES (:blockid, :blockname, :datasetid, :openforwriting, :originsite, :blocksize, :filecount, :creationdate, :createby, :lastmodificationdate, :lastmodifiedby);"""

    def getBinds( self, blocksObj ):
            binds = {}
            if type(blocksObj) == type ('object'):
            	binds = {
			'blockid' : blocksObj['blockid'],
			'blockname' : blocksObj['blockname'],
			'datasetid' : blocksObj['datasetid'],
			'openforwriting' : blocksObj['openforwriting'],
			'originsite' : blocksObj['originsite'],
			'blocksize' : blocksObj['blocksize'],
			'filecount' : blocksObj['filecount'],
			'creationdate' : blocksObj['creationdate'],
			'createby' : blocksObj['createby'],
			'lastmodificationdate' : blocksObj['lastmodificationdate'],
			'lastmodifiedby' : blocksObj['lastmodifiedby'],
                 }

            elif type(blocksObj) == type([]):
               binds = []
               for item in blocksObj:
                   binds.append({
 	                'blockid' : item['blockid'],
 	                'blockname' : item['blockname'],
 	                'datasetid' : item['datasetid'],
 	                'openforwriting' : item['openforwriting'],
 	                'originsite' : item['originsite'],
 	                'blocksize' : item['blocksize'],
 	                'filecount' : item['filecount'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'lastmodificationdate' : item['lastmodificationdate'],
 	                'lastmodifiedby' : item['lastmodifiedby'],
 	                })
               return binds


    def execute( self, blocksObj ):
            binds = self.getBinds(blocksObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return