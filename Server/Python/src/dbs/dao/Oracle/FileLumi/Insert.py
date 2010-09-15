# DAO Object for FileLumi table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:27 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO FILE_LUMIS(FILE_LUMI_ID, RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES (:filelumiid, :runnum, :lumisectionnum, :fileid);"""

    def getBinds( self, file_lumisObj ):
            binds = {}
            if type(file_lumisObj) == type ('object'):
            	binds = {
			'filelumiid' : file_lumisObj['filelumiid'],
			'runnum' : file_lumisObj['runnum'],
			'lumisectionnum' : file_lumisObj['lumisectionnum'],
			'fileid' : file_lumisObj['fileid'],
                 }

            elif type(file_lumisObj) == type([]):
               binds = []
               for item in file_lumisObj:
                   binds.append({
 	                'filelumiid' : item['filelumiid'],
 	                'runnum' : item['runnum'],
 	                'lumisectionnum' : item['lumisectionnum'],
 	                'fileid' : item['fileid'],
 	                })
               return binds


    def execute( self, file_lumisObj ):
            binds = self.getBinds(file_lumisObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return