"""
Class to provide data for unit- and integration tests
"""
import cPickle
import getpass
import os.path
import uuid
import time
import os

from sys import getrefcount

def create_dbs_data_provider(data_type='persistent',data_location=None):
    data_types = {'persistent' : DBSDataProvider(DBSPersistentData()),
                  'transient' : DBSDataProvider(DBSTransientData(data_location=data_location))}

    return data_types.get(data_type, None)

def sort_data(data, sort_key):
    return sorted(data, key=lambda entry: entry[sort_key])

def strip_volatile_fields(data):
    volatile_fields = ['block_id','parent_block_id', 'branch_hash_id',
                       'dataset_id', 'parent_dataset_id', 'data_tier_id',
                       'file_id', 'parent_file_id','file_type_id',
                       'primary_ds_id', 'primary_ds_type_id']

    if isinstance(data, list):
        return [strip_volatile_fields(entry) for entry in data]

    for key in data.keys():
        if key in volatile_fields:
            del data[key]

    return data

class DBSTransientData(object):
    """
    All TestCases in a TestSuite using this class are sharing the same unixtime and unique_hash.
    The unixtime and unique_hash is reset, if no instance of this class exists anymore.
    Therefore, it is necessary to delete TestSuites, if one would like to use different unixtime and unique_ids
    """
    unixtime = 0
    unique_hash = 0
    instance_count = 0

    def __init__(self, data_location):
        if self.instance_count == 0:
            self.reset_unique_ids()
        self.__class__.instance_count += 1

        self.username = getpass.getuser()
        self.data = {}
        self.data_location = data_location
        self.template_data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'../data/template_transient_test_data.pkl')

    def __del__(self):
        self.__class__.instance_count -= 1

    def get_data(self, key):
        if not self.data.has_key(key):
            self.load_data(key)

        return self.data.get(key)

    def load_data(self, key):
        test_data_file = file(self.data_location, "r")
        pkl_test_data = cPickle.load(test_data_file)
        test_data_file.close()

        if isinstance(pkl_test_data, dict) and pkl_test_data.has_key(key):
            self.data.update(pkl_test_data)
        else:
            raise TypeError("Input file %s does not contain the right format!" % (self.data_location))

    def save_data(self):
        test_data_file = file(self.data_location, "w")
        pkl_test_data = cPickle.dump(self.data, test_data_file)
        test_data_file.close()

    def generate_data(self, key):
        template_data_file = file(self.template_data_location,'r')
        template_test_data = cPickle.load(template_data_file)

        if not (isinstance(template_test_data, dict) and template_test_data.has_key(key)):
            raise TypeError("Template file %s does not contain the right format!" % (self.template_data_location))

        template_data = template_test_data.get(key)

        generated_data = []

        for list_entry in template_data:
            for entry,value in list_entry.iteritems():
                if isinstance(value,str):
                    if value.find("@unique_id@") != -1:
                        list_entry[entry] = list_entry[entry].replace("@unique_id@", self.unixtime)
                    if value.find("@date@") != -1:
                        list_entry[entry] = list_entry[entry].replace("@date@", self.unixtime)
                    if value.find("@user@") != -1:
                        list_entry[entry] = list_entry[entry].replace("@user@", self.username)
                    if value.find("@unique_hash@") != -1:
                        list_entry[entry] = list_entry[entry].replace("@unique_hash@", self.unique_hash)
                    if value.find("@unique_id_9999@") != -1:
                        list_entry[entry] = list_entry[entry].replace("@unique_id_9999@", str(int(self.unixtime)%9999))

                    #check if string contains only digits, since DBS3 returns int's in that case,
                    #except for md5, adler32 and checksum
                    if list_entry[entry].isdigit() and entry not in ['md5','adler32','check_sum']:
                        list_entry[entry] = int(list_entry[entry])

            generated_data.append(list_entry)

        generated_data = {key : generated_data}

        self.data.update(generated_data)
        self.save_data()

    @classmethod
    def reset_unique_ids(cls):
        cls.unixtime = str(int(time.time()))
        cls.unique_hash = str(uuid.uuid1()).replace('-','')

class DBSPersistentData(object):
    def __init__(self, data_location=None):
        self.data = {}

        if data_location:
            self.data_location = data_location
        else:
            self.data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data/persistent_test_data.pkl')

    def get_data(self, key):
        if not self.data.has_key(key):
            self.load_data(key)

        return self.data.get(key)

    def load_data(self, key):
        test_data_file = file(self.data_location, "r")
        pkl_test_data = cPickle.load(test_data_file)
        test_data_file.close()

        if isinstance(pkl_test_data, dict) and pkl_test_data.has_key(key):
            self.data.update(pkl_test_data)
        else:
            raise TypeError("Input file %s does not have the right format or does not contain key %s!" % (self.data_location, key))

    def save_data(self):
        raise NotImplemented("You cannot overwrite persistent data!")

    def generate_data(self, key):
        raise NotImplemented("You cannot re-generate persistent data!")

class DBSDataProvider(object):
    def __init__(self, data_store):
        self.data_store = data_store

    def get_acquisition_era_data(self, regenerate=False):
        return self.get_data(key="acquisition_era", regenerate=regenerate)

    def get_block_data(self, regenerate=False):
        return self.get_data(key="block", regenerate=regenerate)

    def get_block_parentage_data(self, regenerate=False):
        return self.get_data(key="block_parentage", regenerate=regenerate)

    def get_child_block_data(self, regenerate=False):
        return self.get_data(key="child_block", regenerate=regenerate)

    def get_child_dataset_data(self, regenerate=False):
        return self.get_data(key="child_dataset", regenerate=regenerate)

    def get_child_file_data(self, regenerate=False):
        return self.get_data(key="child_file", regenerate=regenerate)

    def get_dataset_data(self, regenerate=False):
        return self.get_data(key="dataset", regenerate=regenerate)

    def get_dataset_parentage_data(self, regenerate=False):
        return self.get_data(key="dataset_parentage", regenerate=regenerate)

    def get_data_tier_data(self, regenerate=False):
        return self.get_data(key="data_tier", regenerate=regenerate)

    def get_file_data(self, regenerate=False):
        return self.get_data(key="file", regenerate=regenerate)

    def get_file_lumi_data(self, regenerate=False):
        return self.get_data(key="file_lumi", regenerate=regenerate)

    def get_file_parentage_data(self, regenerate=False):
        return self.get_data(key="file_parentage", regenerate=regenerate)

    def get_output_module_config_data(self, regenerate=False):
        return self.get_data(key="output_module_config", regenerate=regenerate)

    def get_physics_group_data(self, regenerate=False):
        return sort_data(self.get_data(key="physics_group", regenerate=regenerate), 'physics_group_name')

    def get_primary_dataset_data(self, regenerate=False):
        return self.get_data(key="primary_dataset", regenerate=regenerate)

    def get_primary_ds_type_data(self,regenerate=False):
        return sort_data(self.get_data(key="primary_ds_type", regenerate=regenerate), 'data_type')

    def get_processing_era_data(self, regenerate=False):
        return self.get_data(key="processing_era", regenerate=regenerate)

    def get_data(self, key, regenerate):
        if regenerate:
            self.data_store.generate_data(key=key)

        return self.data_store.get_data(key=key)
