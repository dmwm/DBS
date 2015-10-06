"""
File.Insert unittests
"""

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.Block.List import List as BlockList
from dbs.dao.Oracle.File.Insert import Insert as FileInsert
from dbs.dao.Oracle.Dataset.GetID import GetID as DatasetGetID
from dbs.dao.Oracle.FileType.GetID import GetID as FileTypeGetID
from dbs.dao.Oracle.SequenceManager import SequenceManager as SequenceManager

class Insert_t(unittest.TestCase):
    @DaoConfig("DBSWriter")
    def __init__(self, methodName='runTest'):
        super(Insert_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.data = self.data_provider.get_file_data(regenerate=True)[0]
        self.child_data = self.data_provider.get_child_file_data(regenerate=True)[0]
        
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.file_insert = FileInsert(self.logger, self.dbi, self.dbowner)
        self.block_list = BlockList(self.logger, self.dbi, self.dbowner)
        self.dataset_id = DatasetGetID(self.logger, self.dbi, self.dbowner)
        self.file_type_id = FileTypeGetID(self.logger, self.dbi, self.dbowner)
        self.sequence_manager = SequenceManager(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def _insertFile(self, data):
        tran = self.conn.begin()
        
        try:
            data["file_id"] = self.sequence_manager.increment(self.conn, "SEQ_FL", transaction=tran)
            #insert needs an id not the name, whereas list will return the name
            data["dataset_id"] = self.dataset_id.execute(self.conn, dataset=data["dataset"], transaction=tran)
            del data["dataset"]

            #insert needs an id not the name, whereas list will return the name
            block_info = self.block_list.execute(self.conn, block_name=data["block_name"], transaction=tran)
	    for b in block_info:	
		data["block_id"] = b["block_id"]
            del data["block_name"]
            
            #insert needs an id not the name, whereas list will return the name
            data["file_type_id"] = self.file_type_id.execute(self.conn, data["file_type"], transaction=tran)
            del data["file_type"]

            #No more supported, see Ticket #965 YG 
            del data["creation_date"]
            del data["create_by"]
            
            self.file_insert.execute(self.conn, data, transaction=tran)

        except Exception as ex:
            tran.rollback()
            raise ex
        else:
            tran.commit()
        finally:
            if tran:
                tran.close()
    def test01(self):
        """dao.Oracle.File.Insert: Basic"""
        self._insertFile(self.data)

    def test02(self):
        """dao.Oracle.File.Insert: ChildFile"""
        self._insertFile(self.child_data)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(Insert_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
