#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options

import random, os, sys

def change_cksums(block_dict, file_dict):
    file_dict['check_sum'] = str(random.randint(1000, 9999))
    file_dict['adler32'] = str(random.randint(1000, 9999))

def change_file_size(block_dict, file_dict):
    block = block_dict['block']
    old_file_size = file_dict['file_size']
    block['block_size'] -= old_file_size
    new_file_size = int(random.gauss(1000000000,90000000))
    block['block_size'] += new_file_size
    file_dict['file_size'] = new_file_size

def skip_file(block_dict, file_dict):
    logical_file_name = file_dict['logical_file_name']
    file_size = file_dict['file_size']
    block = block_dict['block']
    block['block_size'] -= file_size
    block['file_count'] -= 1
    
    for count, this_file in enumerate(block_dict['files']):
        if this_file['logical_file_name'] == logical_file_name:
            del block_dict['files'][count]

    for count, file_conf in enumerate(block_dict['file_conf_list']):
        if file_conf['lfn'] == logical_file_name:
            del block_dict['file_conf_list'][count]

failure_func = {"DBSSkipFileFail" : skip_file,
                "DBSChangeCksumFail" : change_cksums,
                "DBSChangeSizeFail" : change_file_size}

options = get_command_line_options(__name__, sys.argv)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

block_dump = payload_handler.payload['workflow']['DBS']

for block in block_dump:
    for this_file in block['files']:
        ###get last part of the logical_file_name, which is the actually
        filename = this_file['logical_file_name'].split('/')[-1]

        ###remove .root from filename
        filename = filename.replace('.root', '')
        
        ###decode failures from filename
        failures = filename.split('_')[1:]

        for failure in failures:
            if failure.startswith('DBS'):
                try:
                    ### call function to modify the block contents
                    failure_func[failure](block, this_file)
                except Exception as ex:
                    print "%s does not support the failure %s" % (os.path.basename(__file__), failure)
                    raise ex

p = payload_handler.clone_payload()
p['workflow']['DBS'] = block_dump
payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
