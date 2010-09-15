import cx_Oracle
import threading
import time
import os

class TestcxOracle(threading.Thread):
    def __init__(self, dburl, dbowner):
        threading.Thread.__init__(self)
        self.dbowner = '%s.'  % dbowner
        self.dburl = self.geturl(dburl)
        self.sql = """
SELECT F.FILE_ID, F.LOGICAL_FILE_NAME, F.IS_FILE_VALID, 
        F.DATASET_ID, D.DATASET,
        F.BLOCK_ID, B.BLOCK_NAME, 
        F.FILE_TYPE_ID, FT.FILE_TYPE,
        F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE,  
        F.BRANCH_HASH_ID, F.ADLER32, F.MD5, 
        F.AUTO_CROSS_SECTION,
        F.CREATION_DATE, F.CREATE_BY, 
        F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
FROM %sFILES F 
JOIN %sFILE_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID 
JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID 
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
WHERE D.DATASET = :dataset
""" % (((self.dbowner,)*4))

    def geturl(self, dburl):
        dburl1 = dburl.strip('oracle://')
        return dburl1.replace(':', '/', 1)

    def run(self):
        t = time.time()
        connection = cx_Oracle.connect(self.dburl, threaded = True)
        cursor = connection.cursor()
        cursor.execute(self.sql, {"dataset":"/GlobalAug07-C/Online/RAW"})
        desc = cursor.description
        keys = [desc[i][0] for i in range(len(desc))]
        for r in cursor.fetchall():
            dict(zip(keys, r))
        cursor.close()
        connection.close()
        print "Time: %s " % (time.time() - t)


if __name__ == "__main__":
    for i in range(10):
        dburl = os.environ['DBS_TEST_DBURL_READER']
        dbowner = os.environ['DBS_TEST_DBOWNER_READER']
        current = TestcxOracle(dburl, dbowner)
        current.start()

