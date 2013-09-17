"""
Class to provide data for unit- and integration tests
"""
from itertools import izip
from collections import defaultdict
import cPickle as pickle
import getpass
import os.path
import uuid
import time
import os
import random

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
        pkl_test_data = pickle.load(test_data_file)
        test_data_file.close()

        if isinstance(pkl_test_data, dict) and pkl_test_data.has_key(key):
            self.data.update(pkl_test_data)
        else:
            raise TypeError("Input file %s does not contain the right format!" % (self.data_location))

    def save_data(self):
        test_data_file = file(self.data_location, "w")
        pkl_test_data = pickle.dump(self.data, test_data_file)
        test_data_file.close()

    def generate_data(self, key):
        template_data_file = file(self.template_data_location,'r')
        template_test_data = pickle.load(template_data_file)

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
        pkl_test_data = pickle.load(test_data_file)
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

def create_child_data_provider(parent_data_provider):
    parameters = (parent_data_provider._num_of_blocks,
                  parent_data_provider._num_of_files,
                  parent_data_provider._num_of_runs,
                  parent_data_provider._num_of_lumis)

    child_data_provider = DBSBlockDataProvider(*parameters)

    parent_block_dump = parent_data_provider.block_dump()
    child_block_dump = child_data_provider.block_dump()

    for parent_block, child_block in izip(parent_block_dump, child_block_dump):
        parent_logical_file_names = (this_file['logical_file_name'] for this_file in parent_block['files'])
        child_logical_file_names = (this_file['logical_file_name'] for this_file in child_block['files'])

        file_parent_list = []

        for parent_logical_file_name, child_logical_file_name in izip(parent_logical_file_names,
                                                                      child_logical_file_names):
            file_parent_list.append(dict(parent_logical_file_name=parent_logical_file_name,
                                         logical_file_name=child_logical_file_name))

        child_data_provider.file_parent_list(child_block['block']['block_name'],
                                             file_parent_list)

    return child_data_provider

class DBSBlockDataProvider(object):
    def __init__(self, num_of_blocks=1, num_of_files=10, num_of_runs=10, num_of_lumis=10):
        self._num_of_blocks = num_of_blocks
        self._num_of_files = num_of_files
        self._num_of_runs = num_of_runs
        self._num_of_lumis = num_of_lumis
        self._uid = uuid.uuid4().time_mid
        self._tiers = ('RAW', 'GEN', 'SIM', 'RECO', 'AOD')

        #set starting values for the run number and lumi section to avoid duplicated entries in a block
        self._run_num  = random.randint(1, 100)
        self._lumi_sec = random.randint(1, 100)

        self._files = {}
        self._file_parents = defaultdict(list)

    def load(self, filename):
        """Deserialize object from persistent data storage"""
        with open(filename, 'r') as f:
            self.__dict__ = pickle.load(f)

    def save(self, filename):
        """Serialize object for persistent data storage"""
        with open(filename, 'w') as f:
            pickle.dump(self.__dict__, f)

    def reset(self):
        init_parameters = (self._num_of_blocks, self._num_of_files,
                           self._num_of_runs, self._num_of_lumis)
        self.__dict__ = {}
        #re-initialise values
        self.__init__(*init_parameters)

    def block_dump(self):
        ret_val = []
        for block_name in self.blocks:
            files = self.files(block_name)
            logical_file_names = (this_file['logical_file_name'] for this_file in files)
            file_conf_list = [self._generate_file_conf(lfn) for lfn in logical_file_names]
            ret_val.append( \
                {'dataset_conf_list': [{'release_version' : self.release_version,
                                        'pset_hash' : self.pset_hash,
                                        'app_name' : self.app_name,
                                        'output_module_label' : self.output_module_label,
                                        'global_tag' : self.global_tag}],
                 'file_conf_list' : file_conf_list,
                 'files' : files,
                 'processing_era' : self.processing_era,
                 'primds' : self.primds,
                 'dataset':{'physics_group_name': self.physics_group_name,
                            'dataset_access_type': self.dataset_access_type,
                            'data_tier_name': self.tier,
                            'processed_ds_name': self.processed_dataset,
                            'xtcrosssection': self.xtc_cross_section,
                            'dataset': self.dataset_name},
                 'acquisition_era': self.acquisition_era,
                 'block': {'open_for_writing': self.block_is_open(block_name),
                           'block_name': block_name,
                           'file_count': len(files),
                           'origin_site_name': self.origin_site_name,
                           'block_size': sum((f['file_size'] for f in files))},
                  'file_parent_list': self.file_parent_list(block_name)
                  })
        return ret_val

    def files(self, block_name):
        if not (hasattr(self, '_files') and self._files.has_key(block_name)):
            self._files[block_name] = []
            num_of_created_blocks = len(self._files)
            for i in xrange((num_of_created_blocks-1) * self._num_of_files,
                            num_of_created_blocks * self._num_of_files):
                logical_file_name = self._generate_file_name(i)
                self._files[block_name].append({'check_sum' : self._generate_cksum(),
                                                'file_size' : self._generate_file_size(),
                                                'file_lumi_list' : self._generate_file_lumi_list(),
                                                'adler32' : self._generate_adler32(),
                                                'event_count' : self._generate_event_count(),
                                                'file_type' : 'EDM',
                                                'logical_file_name' : logical_file_name,
                                                'md5' : None,
                                                'auto_cross_section' : self._generate_auto_cross_section()
                                                })
        return self._files[block_name]

    def file_parent_list(self, block_name, file_parent_list=None):
        """
        Combined Setter and getter function for self._file_parent
        self._file_parents is a defaultdict returning [] if the key is not present.
        Once the value is set to file_parent_list, it will return the value instead.
        """
        if file_parent_list:
            self._file_parents[block_name] = list(file_parent_list)

        return self._file_parents[block_name]

    def _generate_adler32(self):
        "generates adler32 checksum"
        return random.randint(1000, 9999)

    def _generate_auto_cross_section(self):
        "generate auto cross section for a given file, if not already available"
        return random.uniform(0.0, 100.0)

    def _generate_block_name(self):
        "generates new block name"
        return '/%s/%s/%s#%s' % (self.primary_ds_name,
                                 self.processed_dataset,
                                 self.tier,
                                 uuid.uuid4())

    def _generate_block_is_open(self):
        "generates block is open status"
        return random.randint(0,1)

    def _generate_cksum(self):
        "generates checksum"
        return random.randint(1000,9999)

    def _generate_event_count(self):
        "generate event count for a given file, if not already available"
        return random.randint(10, 10000)

    def _generate_file_conf(self, logical_file_name):
        return {'release_version': self.release_version,
                'pset_hash': self.pset_hash,
                'lfn': logical_file_name,
                'app_name': self.app_name,
                'output_module_label': self.output_module_label,
                'global_tag': self.global_tag}

    def _generate_file_name(self, file_counter):
        "generates new file name"
        counter = str(0).zfill(9)
        return '/store/data/%s/%s/%s/%s/%s/%s_%s.root' % \
            (self.acquisition_era_name,
             self.primary_ds_name,
             self.tier,
             self.processing_version,
             counter,
             self._uid,
             file_counter)

    def _generate_file_size(self, func='gauss', params=(1000000000,90000000)):
        "generates new file size"
        return int(abs(getattr(random,func)(*params)))

    def _generate_file_lumi_list(self):
        "generate file lumi list for a given file, if not already available"
        output = []
        for _ in xrange(0, self._num_of_runs):
            self._run_num += 1
            for _ in range(0, self._num_of_lumis):
                self._lumi_sec += 1
                row = dict(run_num=self._run_num, lumi_section_num=self._lumi_sec)
                output.append(row)
        return output

    @property
    def acquisition_era_name(self):
        "return acquisition era name"
        if not hasattr(self, '_acquisition_era_name'):
            self._acquisition_era_name = "acq_era_%s" % self._uid
        return self._acquisition_era_name

    @property
    def acquisition_era(self):
        "return acquisition era object"
        if not hasattr(self, '_acquisition_era'):
            self._acquisition_era = {"acquisition_era_name": self.acquisition_era_name,
                                     'start_date': 1234567890,
                                     "description": "Test_acquisition_era"}
        return self._acquisition_era

    @property
    def app_name(self):
        "return application name"
        if not hasattr(self, '_app_name'):
            self._app_name = 'cmsRun%s' % self._uid
        return self._app_name

    @property
    def blocks(self):
        "return list of blocks"
        if not hasattr(self, '_blocks'):
            self._blocks = []
            for i in xrange(self._num_of_blocks):
                self._blocks.append(self._generate_block_name())
        return self._blocks

    def block_is_open(self, block_name):
        "return block is open"
        if not hasattr(self, '_block_is_open'):
            self._block_is_open = {block_name : self._generate_block_is_open()}
        elif not self._block_is_open.has_key(block_name):
            self._block_is_open.update({block_name : self._generate_block_is_open()})

        return self._block_is_open[block_name]

    @property
    def dataset_access_type(self):
        "return dataset access type"
        if not hasattr(self,'_dataset_access_type'):
            self._dataset_access_type = "VALID"
        return self._dataset_access_type

    @property
    def dataset_name(self):
        "return dataset name"
        if not hasattr(self, "_dataset_name"):
            self._dataset_name = '/%s/%s/%s' % \
                    (self.primary_ds_name,
                     self.processed_dataset,
                     self.tier)
        return self._dataset_name

    @property
    def global_tag(self):
        "return global tag"
        if not hasattr(self, '_global_tag'):
            self._global_tag = 'dbs-unit-test-%s' % self._uid
        return self._global_tag

    @property
    def origin_site_name(self):
        "return origin site name"
        if not hasattr(self, '_origin_site_name'):
            self._origin_site_name = 'grid-srm.physik.rwth-aachen.de'
        return self._origin_site_name

    @property
    def output_config(self):
        "Generate DBS output config meta-data"
        rec  = dict(configs=\
                    dict(release_version=self.release_version, pset_hash=self.pset_hash, app_name=self.app_name,
                         output_module_label=self.output_module_label, global_tag=self.global_tag))
        return rec

    @property
    def output_module_label(self):
        "return output module label"
        if not hasattr(self, '_output_module_label'):
            self._output_module_label = 'Merged'
        return self._output_module_label

    @property
    def physics_group_name(self):
        "return physics group name"
        if not hasattr(self, "_physics_group_name"):
            self._physics_group_name = "Tracker"
        return self._physics_group_name

    @property
    def primary_ds_name(self):
        "return primary dataset name"
        if not hasattr(self, '_primary_ds_name'):
            self._primary_ds_name = 'unittest_web_primary_ds_name_%s' % self._uid
        return self._primary_ds_name

    @property
    def primary_ds_type(self):
        "return primary dataset type"
        if not hasattr(self, '_primary_ds_type'):
            primary_ds_types = ['mc', 'data']
            self._primary_ds_type = primary_ds_types[random.randint(0,1)]
        return self._primary_ds_type

    @property
    def primds(self):
        "return primary dataset object"
        if not hasattr(self, '_primds'):
            self._primds = {"primary_ds_type": self.primary_ds_type,
                            "primary_ds_name": self.primary_ds_name}
        return self._primds

    @property
    def processed_dataset_name(self):
        "return processed dataset name"
        if not hasattr(self, '_processed_dataset_name'):
            self._processed_dataset_name = 'unittest_web_dataset'
        return self._processed_dataset_name

    @property
    def processed_dataset(self):
        "return processed dataset path"
        if not hasattr(self, '_processed_dataset'):
            self._processed_dataset = '%s-%s-v%s' % \
                (self.acquisition_era_name,
                 self.processed_dataset_name,
                 self.processing_version)
        return self._processed_dataset

    @property
    def processing_era(self):
        "return processing era object"
        if not hasattr(self, '_processing_era'):
            self._processing_era = {"processing_version": self.processing_version,
                                    "description": "Test_proc_era"}
        return self._processing_era

    @property
    def pset_hash(self):
        "return parameter set hash"
        if not hasattr(self, '_pset_hash'):
            self._pset_hash = '76e303993a1c2f842159dbfeeed9a0dd%s' % self._uid
        return self._pset_hash

    @property
    def processing_version(self):
        "return processing version"
        if not hasattr(self, '_processing_version'):
            self._processing_version = random.randint(1, 100)
        return self._processing_version

    @property
    def release_version(self):
        "return release version"
        if not hasattr(self, '_release_version'):
            self._release_version = 'CMSSW_1_2_%s' % self._uid
        return self._release_version

    @property
    def tier(self):
        "return tier name"
        if not hasattr(self, '_tier'):
            self._tier = self._tiers[random.randint(0, len(self._tiers)-1)]
        return self._tier

    @property
    def xtc_cross_section(self):
        "return cross section value"
        if not hasattr(self, '_xtc_cross_section'):
            self._xtc_cross_section = random.uniform(0.0, 1000.0)
        return self._xtc_cross_section
