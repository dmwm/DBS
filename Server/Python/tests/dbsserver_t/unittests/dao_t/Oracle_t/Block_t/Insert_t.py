"""
Block.Insert unittests
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.Block.Insert import Insert as BlockInsert
from dbs.dao.Oracle.Dataset.GetID import GetID as DatasetGetID
from dbs.dao.Oracle.SequenceManager import SequenceManager as SequenceManager

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.data = self.data_provider.get_block_data(regenerate=True)[0]
        self.child_data = self.data_provider.get_child_block_data(regenerate=True)[0]
         
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.block_insert = BlockInsert(self.logger, self.dbi, self.dbowner)
        self.dataset_id = DatasetGetID(self.logger, self.dbi, self.dbowner)
        self.sequence_manager = SequenceManager(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def _insertBlock(self, data):
        tran = self.conn.begin()
        
        try:
            ds_name = data["dataset"]
            data["dataset_id"] = self.dataset_id.execute(self.conn, ds_name, tran)
            del data["dataset"]
            data["block_id"] =  self.sequence_manager.increment(self.conn, "SEQ_BK", tran)
            self.block_insert.execute(self.conn, data, tran)

        except Exception as ex:
            tran.rollback()
            raise ex
        else:
            tran.commit()
        finally:
            if tran:
                tran.close()
                
    def test01(self):
        """dao.Oracle.Block.Insert: Basic"""
        self._insertBlock(self.data)

    def test02(self):
        """dao.Oracle.Block.Insert: ChildBlock"""
        self._insertBlock(self.child_data)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(Insert_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
