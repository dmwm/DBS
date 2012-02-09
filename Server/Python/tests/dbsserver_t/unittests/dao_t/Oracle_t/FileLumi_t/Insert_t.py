"""
FileLumi.Insert unittest
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.FileLumi.Insert import Insert as FileLumiInsert
from dbs.dao.Oracle.File.GetID import GetID as FileID

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.lumi_data = self.data_provider.get_file_lumi_data(regenerate=True)[0]
                 
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.file_lumi_insert = FileLumiInsert(self.logger, self.dbi, self.dbowner)
        self.file_id = FileID(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def test01(self):
        """dao.Oracle.FileLumi.Insert: Basic"""
        tran = self.conn.begin()
        
        try:
            self.lumi_data["file_id"] = self.file_id.execute(self.conn, self.lumi_data["logical_file_name"], tran)
            del self.lumi_data["logical_file_name"]
            
            self.file_lumi_insert.execute(self.conn, self.lumi_data)
        except Exception as ex:
            tran.rollback()
            raise ex
        else:
            tran.commit()
        finally:
            if tran:
                tran.close()
