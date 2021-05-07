from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

class DBApi(object):
    def __init__(self, logger, connectUrl, owner):
        object.__init__(self)

        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()
        self.dbFormatter = DBFormatter(logger, self.dbi)
        self.owner = owner
        
        self.sqlDict = {'trig':
                        """
                        SELECT TABLE_NAME, TRIGGER_BODY
                        FROM USER_TRIGGERS
                        WHERE TABLE_OWNER='%s'
                        """ % (owner),
                        'primaryKey':
                        """
                        SELECT cols.table_name table_name, cols.column_name primaryk
                        FROM all_constraints cons, all_cons_columns cols
                        WHERE cols.table_name = :table_name AND cons.OWNER='%s'
                        AND cons.constraint_type = 'P'
                        AND cons.constraint_name = cols.constraint_name
                        AND cons.owner = cols.owner
                        """ % (owner), 
                        'sequen':
                        """
                        SELECT INCREMENT_BY inc, CACHE_SIZE csz from USER_SEQUENCES 
                        where SEQUENCE_NAME=:seq_name
                        """ 
                        }
        

    def getTrig(self):
        conn = self.dbi.connection()
        binds={}
        results = self._queryDB('trig', binds, conn)
        #print results
        conn.close()
        for i in results:
            binds[i['table_name']] = i['trigger_body'][i['trigger_body'].find('SEQ_'):\
                        i['trigger_body'].find('.nextval') ]
        return binds
                
    def getPrimaryKey(self, table_name):
        conn = self.dbi.connection()
        binds={}
        binds['table_name']=table_name
        results = self._queryDB('primaryKey', binds, conn)
        conn.close()
        return results
        
    def getSequence(self, seq_name):
        conn = self.dbi.connection()
        binds={}
        binds['seq_name']=seq_name
        results = self._queryDB('sequen', binds, conn)
        conn.close()
        return results
    
    def getMaxID(self, primaryK, tableName):
        sql = "SELECT MAX(%s) maxid FROM %s.%s" %(primaryK, self.owner, tableName)
        conn = self.dbi.connection()
        cursors = self.dbi.processData(sql, {}, conn, 
                                    transaction=False, returnCursor=True)
        conn.close()
        return self.dbFormatter.formatCursor(cursors[0])   
 
    def _queryDB(self,query,binds, conn, sort=True):
        
        cursors = self.dbi.processData(self.sqlDict[query], binds, conn,
                                       transaction=False, returnCursor=True)
        
        return self.dbFormatter.formatCursor(cursors[0])

def generator():
    #modify db info based on your schema
    ownerDBS3 = 'owner'
    connectUrlDBS3 = 'oracle://account:pd@mydb'
    role = 'CMS_DBS3_INT_GLOBAL_R_ROLE'

    output = open('recreateSequence.sql', 'w') 
    logger = logging.getLogger()
    dbapi = DBApi(logger, connectUrlDBS3, ownerDBS3)
    results = dbapi.getTrig()
    for t, s in results.items():
        #print '\n **********'
        #print t, s
        primary = dbapi.getPrimaryKey(t)
        #print primary
        maxID = dbapi.getMaxID(primary[0]['primaryk'], primary[0]['table_name'])[0]['maxid']
        #print maxID   
        if maxID:
           sql1 = 'DROP SEQUENCE %s.%s;' %(ownerDBS3, s)   
           seqV = dbapi.getSequence(s)
           #print seqV
           sql2 = """CREATE SEQUENCE %s.%s
                  START WITH %s
                  INCREMENT BY %s
                  MAXVALUE 999999999999999999999999999
                  MINVALUE %s
                  NOCYCLE
                  CACHE %s
                  NOORDER;""" %(ownerDBS3, s, int(maxID)+1, seqV[0]['inc'], maxID, seqV[0]['csz'] )
           sql3 = """ GRANT SELECT ON %s.%s TO %s;""" %(ownerDBS3, s, role) 
           output.write(sql1+'\n')
           output.write(sql2+'\n')
           output.write(sql3+'\n\n')
           #print sql1
           #print sql2      
    output.close()
if __name__ == '__main__':
    import logging
    generator()

