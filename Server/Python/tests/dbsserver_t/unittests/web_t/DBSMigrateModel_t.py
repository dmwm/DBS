#!/usr/bin/env python
"""
DBS 3 Migrate REST model unittests

The DBS3 Migration Service must be stopped before executing the unittest. In addition, take care
that no instance is running on the same DB. Else the single unittests can happen to fail due to
race conditions with DBS3 Migration Service.
"""
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from dbsserver_t.utils.DBSDataProvider import DBSBlockDataProvider, create_child_data_provider
from dbsserver_t.utils.TestTools import expectedFailure

from itertools import chain

import os
import socket
import unittest


class DBSMigrateModel_t(unittest.TestCase):
    _data_provider = None
    _saved_data = {}

    def __init__(self, methodName='runTest'):
        super(DBSMigrateModel_t, self).__init__(methodName)
        if not self._data_provider:
            self.setUpClass()

    @classmethod
    def setUpClass(cls):
        cls._data_provider = DBSBlockDataProvider(num_of_blocks=1, num_of_files=10, num_of_runs=10, num_of_lumis=10)
        ### According to https://svnweb.cern.ch/trac/CMSDMWM/ticket/4068, blocks and dataset migration should use
        ### separate input data. _independent(_child)_data_provider will provide them.
        cls._independent_data_provider = DBSBlockDataProvider(num_of_blocks=5, num_of_files=10, num_of_runs=10,
                                                              num_of_lumis=10)
        cls._parent_data_provider = DBSBlockDataProvider(num_of_blocks=1, num_of_files=10,
                                                         num_of_runs=10, num_of_lumis=10)
        cls._child_data_provider = create_child_data_provider(cls._parent_data_provider)
        cls._independent_child_data_provider = create_child_data_provider(cls._independent_data_provider)
        config = os.environ['DBS_TEST_CONFIG']
        service = os.environ.get("DBS_TEST_SERVICE", "DBSMigrate")
        cls._migrate_api = DBSRestApi(config, service, migration_test=True)
        cls._migration_url = 'https://%s/dbs/dev/global/DBSWriter' % (socket.getfqdn())
        #Don't remove the commented line below until I have a better way to accommodate the development environment.
        #cls._migration_url = 'http://%s:8787/dbs/dev/global/DBSWriter' % (socket.getfqdn())
        cls._writer_api = DBSRestApi(config, 'DBSWriter')

    def setUp(self):
        pass

    @expectedFailure
    def test_01_migration_removal(self):
        """test01: Clean-up old migration requests. Test to remove migration requests between different DBS instances"""
        for status in sorted(self._migrate_api.list('status'), key=lambda status: status['migration_request_id']):
            data = {'migration_rqst_id': status['migration_request_id']}
            if status['migration_status'] in (0, 3) and status['create_by'] == os.getlogin():
                self._migrate_api.insert('remove', data)
            else:
                self.assertRaises(Exception, self._migrate_api.insert, 'remove', data)
    def test_02_migration_request(self):
        """test02: Negative test to request a migration between different DBS instances before injecting data"""
        for block_name in (block['block']['block_name'] for block in self._child_data_provider.block_dump()):
            toMigrate = {'migration_url' : self._migration_url,
                         'migration_input' : block_name}
            self.assertRaises(Exception, self._migrate_api.insert, 'submit', toMigrate)

    def test_03_insert_data_to_migrate(self):
        """test03: Insert data to migrate into source DBS instance"""
        for block in chain(self._data_provider.block_dump(),
                           self._independent_data_provider.block_dump(),
                           self._parent_data_provider.block_dump(),
                           self._child_data_provider.block_dump(),
                           self._independent_child_data_provider.block_dump()):

            self._writer_api.insert('bulkblocks', block)

    def test_04_migration_request(self):
        """test04: Test to request a migration between different DBS instances by block"""
        for block_name in (block['block']['block_name'] for block in self._child_data_provider.block_dump()):
            toMigrate = {'migration_url' : self._migration_url,
                         'migration_input' : block_name}
            result = self._migrate_api.insert('submit', toMigrate)
            self._saved_data.setdefault('migration_rqst_ids', []).append(result['migration_details']['migration_request_id'])
            self._saved_data.setdefault('migration_inputs', []).append(block_name)

    def test_05_migration_request(self):
        """test05: Test to request a migration between different DBS instances by dataset"""
        datasets = set((block['dataset']['dataset']
                        for block in chain(self._child_data_provider.block_dump(),
                                           self._independent_child_data_provider.block_dump())))
        for dataset in datasets:
            toMigrate = {'migration_url' : self._migration_url,
                         'migration_input' : dataset}
            result = self._migrate_api.insert('submit', toMigrate)
            self._saved_data.setdefault('migration_rqst_ids', []).append(result['migration_details']['migration_request_id'])

    def test_06_migration_status(self):
        """test06: Test to check the status of an ongoing migration between different DBS instances by id"""
        status = self._migrate_api.list('status')
        self.assertTrue(isinstance(status, list))

        for migration_rqst_id in self._saved_data['migration_rqst_ids']:
            status = self._migrate_api.list('status', migration_rqst_id)
            self.assertEqual(len(status), 1)

    def test_07_migration_status(self):
        """test07: Test to check the status of an ongoing migration between different DBS instances by block"""
        for migration_input in self._saved_data['migration_inputs']:
            status = self._migrate_api.list('status', block_name=migration_input)
            self.assertEqual(len(status), 1)

    def test_08_migration_status(self):
        """test08: Test to check the status of an ongoing migration between different DBS instances by dataset"""
        datasets = set((block_name.split('#', 1)[0] for block_name in self._saved_data['migration_inputs']))
        for dataset in datasets:
            status = self._migrate_api.list('status', dataset=dataset)
            self.assertTrue(len(status)>=1)

    def test_09_migration_removal(self):
        """test09: Test to remove a pending migration request between different DBS instances"""
        for migration_rqst_id in self._saved_data['migration_rqst_ids']:
            data = {'migration_rqst_id': migration_rqst_id}
            self._migrate_api.insert('remove', data)

    def test_99_save_data_to_disk(self):
        """test99: Save data to disk to re-use data for migration server unittests"""
        self._data_provider.save('migration_unittest_data.pkl')
        self._independent_data_provider.save('migration_unittest_independent_data.pkl')
        self._parent_data_provider.save('migration_unittest_parent_data.pkl')
        self._independent_child_data_provider.save('migration_unittest_independent_child_data.pkl')
        self._child_data_provider.save('migration_unittest_child_data.pkl')


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSMigrateModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
