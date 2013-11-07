#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from DataProvider.core.dbs_provider import DBSDataProvider
from DataProvider.core.phedex_provider import PhedexDataProvider
from DataProvider.core.data_provider import generate_uid

import random, sys

def block_dump(block):
    # generate dataset configuration info
    rel   = 'CMSSW_1_2_3'
    app   = 'cmsRun'
    tag   = 'TAG'
    label = 'Merged'
    phash = generate_uid(32)
    phys_group = 'Tracker'
    info  = dict(release_version=rel, pset_hash=phash, app_name=app,
                 output_module_label=label, global_tag=tag)

    block_name = block['name']
    dataset_name = block_name.split('#')[0]
    _, primary_ds_name, processed_ds_name, tier = dataset_name.split('/')
    acquisition_era_name, _, processing_version = processed_ds_name.split("-")
    proc_era = {"processing_version": processing_version[1:], #remove v from v4711
                "description": "Test_proc_era"}
    acq_era = {"acquisition_era_name": acquisition_era_name,
               'start_date': 1234567890,
               "description": "Test_acquisition_era"}
    primds = dbs_data_provider.prim_ds(1)[0].get('prim_ds')
    primds.update({"primary_ds_name":primary_ds_name})

    files = []
    file_conf_list = []
    block_size = 0
    
    for this_file in block['files']:
        this_file = this_file['file']
        cksum = this_file['checksum']
        block_size += this_file['bytes']
        files.append({'check_sum': cksum.split(',')[0].split(':')[1],
                      'file_lumi_list': dbs_data_provider.file_lumi_list(),
                      'adler32': cksum.split(',')[1].split(':')[1],
                      'event_count': random.randint(10, 10000),
                      'file_type': 'EDM',
                      'logical_file_name': this_file['name'],
                      'md5': None,
                      'auto_cross_section': 0.0})
        file_conf_list.append({'release_version': rel,
                               'pset_hash': phash,
                               'lfn': this_file['name'],
                               'app_name': app,
                               'output_module_label': label,
                               'global_tag': tag})
    
    block_dump = {'dataset_conf_list': [{'release_version' : rel,
                                         'pset_hash' : phash,
                                         'app_name' : app,
                                         'output_module_label' : label,
                                         'global_tag' : tag}],
                  'file_conf_list' : file_conf_list,
                  'files' : files,
                  'processing_era' : proc_era,
                  'primds' : primds,
                  'dataset':{'physics_group_name': phys_group,
                             'dataset_access_type': 'VALID',
                             'data_tier_name': tier,
                             'processed_ds_name': processed_ds_name,
                             'xtcrosssection': 123.0,
                             'dataset': dataset_name},
                  'acquisition_era': acq_era,
                  'block': {'open_for_writing': block['is-open']=='y',
                            'block_name': block_name,
                            'file_count': block['nfiles'],
                            'origin_site_name': 'grid-srm.physik.rwth-aachen.de',
                            'block_size': block_size},
                  'file_parent_list': []
                  }

    return block_dump

options = get_command_line_options(__name__, sys.argv)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

### read inputs from payload
number_of_datasets = payload_handler.payload['workflow']['NumberOfDatasets']
number_of_blocks = payload_handler.payload['workflow']['NumberOfBlocks']
number_of_files = payload_handler.payload['workflow']['NumberOfFiles']
number_of_runs = payload_handler.payload['workflow']['NumberOfRuns']
number_of_lumis = payload_handler.payload['workflow']['NumberOfLumis']
fail_skip_DBS3 = payload_handler.payload['workflow']['FailSkipDBS3']
fail_cksum_Phedex = payload_handler.payload['workflow']['FailCksumPhedex']
fail_size_Phedex = payload_handler.payload['workflow']['FailSizePhedex']

### initialze DataProvider
dbs_data_provider = DBSDataProvider()
phedex_data_provider = PhedexDataProvider()

### create PhEDEx data
phedex_datasets = phedex_data_provider.datasets(number_of_datasets)
phedex_datasets = [phedex_data_provider.add_blocks(dataset, number_of_blocks) for dataset in phedex_datasets]
phedex_datasets = [phedex_data_provider.add_files(dataset, number_of_files) for dataset in phedex_datasets]

dbs_block_dumps = []

### create DBS3 data
for dataset in phedex_datasets:
    for block in dataset['dataset']['blocks']:
        dbs_block_dumps.append(block_dump(block['block']))

###clone payload, add new data  and save to disk
p = payload_handler.clone_payload()
p['workflow']['Phedex'] = phedex_datasets
p['workflow']['DBS'] = dbs_block_dumps

payload_handler.append_payload(p)
payload_handler.save_payload(options.output)
