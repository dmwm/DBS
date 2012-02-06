"""
PrimaryDataset.Insert unittests
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.PrimaryDataset.Insert import Insert as PrimaryDatasetInsert
from dbs.dao.Oracle.PrimaryDSType.List import List as PrimaryDSTypeList
from dbs.dao.Oracle.SequenceManager import SequenceManager as SequenceManager

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.data = self.data_provider.get_primary_dataset_data(regenerate=True)[0]
         
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.primary_dataset_insert = PrimaryDatasetInsert(self.logger, self.dbi, self.dbowner)
        self.primary_ds_type = PrimaryDSTypeList(self.logger, self.dbi, self.dbowner)
        self.sequence_manager = SequenceManager(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def test01(self):
        """dao.Oracle.PrimaryDataset.Insert: Basic"""
        tran = self.conn.begin()
        
        try:
            self.data["primary_ds_type_id"] = (self.primary_ds_type.execute(self.conn, self.data["primary_ds_type"], transaction=tran))[0]["primary_ds_type_id"] 
            del self.data["primary_ds_type"]
            self.data["primary_ds_id"] = self.sequence_manager.increment(self.conn, "SEQ_PDS", tran)
        
            self.primary_dataset_insert.execute(self.conn, self.data)
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
