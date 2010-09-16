"""Module provides the the class for sql/ tests with cx_Oracle"""

__revision__ = "$Id: CXOracleSQL.py,v 1.1 2010/01/01 19:54:38 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import cx_Oracle
import os
import json

class SQLTester:
    def __init__(self, dburl):
        self.dburl = self.geturl(dburl)
        
    def geturl(self, dburl):
        dburl = dburl.strip('oracle://')
        return dburl.replace(':', '/', 1)
    
    def execute(self, sql, binds={}):
        connection = cx_Oracle.connect(self.dburl) 
        cursor = connection.cursor()
        print sql
        print binds
        if binds:
            cursor.execute(sql, binds)
        else:
            cursor.execute(sql)
        desc = cursor.description
        keys = [desc[i][0] for i in range(len(desc))]
        result = [dict(zip(keys, r)) for r in cursor.fetchall()]
        cursor.close()
        connection.close()
        print json.dumps(result, sort_keys=True, indent=4)

if __name__ == "__main__":

    DBURL = os.environ['DBS_TEST_DBURL_READER']
    DBOWNER = os.environ['DBS_TEST_DBOWNER_READER']
    DBOWNER = '%s.' % DBOWNER
    
    CXDAO = SQLTester(DBURL) 

    SQL = """
SELECT F.LOGICAL_FILE_NAME,
       PF.LOGICAL_FILE_NAME PARENT_LOGICAL_FILE_NAME
FROM FILES F
LEFT OUTER JOIN FILE_PARENTS FP ON FP.THIS_FILE_ID = F.FILE_ID
LEFT OUTER JOIN FILES PF ON PF.FILE_ID = FP.PARENT_FILE_ID
WHERE F.LOGICAL_FILE_NAME = '/store/sut_file_withlumisandparents_29.root'
"""
    BINDS = {}
    CXDAO.execute(SQL, BINDS)


