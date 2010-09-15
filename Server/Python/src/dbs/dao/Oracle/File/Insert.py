#!/usr/bin/env python
""" DAO Object for Files table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:20 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sFILES ( FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID, DATASET_ID, BLOCK_ID, FILE_TYPE_ID, CHECK_SUM, EVENT_COUNT, FILE_SIZE, BRANCH_HASH_ID, ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES (:fileid, :logicalfilename, :isfilevalid, :datasetid, :blockid, :filetypeid, :checksum, :eventcount, :filesize, :branchhashid, :adler32, :md5, :autocrosssection, :creationdate, :createby, :lastmodificationdate, :lastmodifiedby) % (self.owner) ;"""

    def getBinds_delme( self, filesObj ):
            binds = {}
            if type(filesObj) == type ('object'):
            	binds = {
			'fileid' : filesObj['fileid'],
			'logicalfilename' : filesObj['logicalfilename'],
			'isfilevalid' : filesObj['isfilevalid'],
			'datasetid' : filesObj['datasetid'],
			'blockid' : filesObj['blockid'],
			'filetypeid' : filesObj['filetypeid'],
			'checksum' : filesObj['checksum'],
			'eventcount' : filesObj['eventcount'],
			'filesize' : filesObj['filesize'],
			'branchhashid' : filesObj['branchhashid'],
			'adler32' : filesObj['adler32'],
			'md5' : filesObj['md5'],
			'autocrosssection' : filesObj['autocrosssection'],
			'creationdate' : filesObj['creationdate'],
			'createby' : filesObj['createby'],
			'lastmodificationdate' : filesObj['lastmodificationdate'],
			'lastmodifiedby' : filesObj['lastmodifiedby'],
                 }

            elif type(filesObj) == type([]):
               binds = []
               for item in filesObj:
                   binds.append({
 	                'fileid' : item['fileid'],
 	                'logicalfilename' : item['logicalfilename'],
 	                'isfilevalid' : item['isfilevalid'],
 	                'datasetid' : item['datasetid'],
 	                'blockid' : item['blockid'],
 	                'filetypeid' : item['filetypeid'],
 	                'checksum' : item['checksum'],
 	                'eventcount' : item['eventcount'],
 	                'filesize' : item['filesize'],
 	                'branchhashid' : item['branchhashid'],
 	                'adler32' : item['adler32'],
 	                'md5' : item['md5'],
 	                'autocrosssection' : item['autocrosssection'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                'lastmodificationdate' : item['lastmodificationdate'],
 	                'lastmodifiedby' : item['lastmodifiedby'],
 	                })
               return binds


    def execute( self, filesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( filesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


