"""
Dataset.Insert unittests
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.Dataset.Insert import Insert as DatasetInsert
from dbs.dao.Oracle.ProcessingEra.GetID import GetID as ProcessingEraID
from dbs.dao.Oracle.AcquisitionEra.GetID import GetID as AcquisitionEraID
from dbs.dao.Oracle.SequenceManager import SequenceManager as SequenceManager

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.data = self.data_provider.get_dataset_data(regenerate=True)[0]
        self.child_data = self.data_provider.get_child_dataset_data(regenerate=True)[0]
         
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dataset_insert = DatasetInsert(self.logger, self.dbi, self.dbowner)
        self.processing_era_id = ProcessingEraID(self.logger, self.dbi, self.dbowner)
        self.acquisition_era_id = AcquisitionEraID(self.logger, self.dbi, self.dbowner)
        self.sequence_manager = SequenceManager(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def _insertDataset(self, data):
        tran = self.conn.begin()
        
        try:
            data["dataset_id"] = self.sequence_manager.increment(self.conn, "SEQ_DS", tran)
            data["data_tier_name"] =  data["data_tier_name"].upper()
            data["dataset_access_type"] = data["dataset_access_type"].upper()

            #insert needs an id not the name, whereas list will return the name
            data["physics_group_id"] = None
            del data["physics_group_name"]

            #insert needs an id not the name, whereas list will return the name
            data["processing_era_id"] = self.processing_era_id.execute(self.conn, data["processing_version"], tran)
            del data["processing_version"]

            #insert needs an id not the name, whereas list will return the name
            data["acquisition_era_id"] = self.acquisition_era_id.execute(self.conn, data["acquisition_era_name"], tran) 
            del data["acquisition_era_name"]

            #not needed for the insert, but it is returned by list api
            del data["primary_ds_type"]
            
            self.dataset_insert.execute(self.conn, data, tran)
        except Exception as ex:
            tran.rollback()
            raise ex
        else:
            tran.commit()
        finally:
            if tran:
                tran.close()

    def test01(self):
        """dao.Oracle.Dataset.Insert: Basic"""
        self._insertDataset(self.data)
        
    def test02(self):
        """dao.Oracle.Dataset.Insert: ChildDataset"""
        self._insertDataset(self.child_data)
                
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(Insert_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
