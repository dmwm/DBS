"""
DataTier.Insert unittests
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.DataTier.Insert import Insert as DataTierInsert
from dbs.dao.Oracle.SequenceManager import SequenceManager as SequenceManager

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.data = self.data_provider.get_data_tier_data(regenerate=True)[0]
         
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.data_tier_insert = DataTierInsert(self.logger, self.dbi, self.dbowner)
        self.sequence_manager = SequenceManager(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def test01(self):
        """dao.Oracle.DataTier.Insert: Basic"""
        tran = self.conn.begin()
        
        try:
            self.data["data_tier_id"] = self.sequence_manager.increment(self.conn, "SEQ_DT", tran)
            self.data["data_tier_name"] = self.data["data_tier_name"].upper()
            self.data_tier_insert.execute(self.conn, self.data, tran)
        except Exception as ex:
            tran.rollback()
            raise ex
        else:
            tran.commit()
        finally:
            if tran:
                tran.close()

        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(Insert_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
