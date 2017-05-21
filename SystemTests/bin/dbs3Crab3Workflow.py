#!/usr/bin/env python

from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys
import tempfile

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL", "https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi', config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

named_pipe = payload_handler.payload['workflow']['NamedPipe']

stat_client = StatsPipeClient(named_pipe)

initial = payload_handler.payload['workflow']['dataset']

## list primary data type
timing = {'stats':{'query' : initial, 'api' : 'listPrimaryDSTypes'}}
with TimingStat(timing, stat_client) as timer:
    ds_type = api.listPrimaryDSTypes(dataset=initial)[0]

request_processing_time, request_time = api.requestTimingInfo
timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                    'server_request_timestamp' : float(request_time)/1000000.0,
                    'request_content_length' : int(api.requestContentLength)})

timer.stat_to_server()

print("PrimaryDSType is %s" % (ds_type))

## list all files in DBS3 for a given dataset
timing.get('stats').update({'api' : 'listFiles'})
with TimingStat(timing, stat_client) as timer:
  files = api.listFiles(dataset=initial, detail=True)

request_processing_time, request_time = api.requestTimingInfo
timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                    'server_request_timestamp' : float(request_time)/1000000.0,
                    'request_content_length' : int(api.requestContentLength)})

timer.stat_to_server()

print("Found %s files for dataset %s" % (len(files), initial))

### get blocks for the dataset
blocks = set()
block_ids = set()
for this_file in files:
    blocks.add(this_file['block_name'])
    block_ids.add(this_file['block_id'])

## list parent_files and file_lumis for all the files in blocks
for block, block_id in zip(blocks, block_ids):
    timing.get('stats').update({'api' : 'listFileParents','query' : str(block_id)})
    with TimingStat(timing, stat_client) as timer:
        parent_files = api.listFileParents(block_id=block_id)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                        'server_request_timestamp' : float(request_time)/1000000.0,
                        'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server()
    
    print("Found %s parents for files in block  %s" % (len(parent_files), block))

    timing.get('stats').update({'api' : 'listFileLumis','query' : str(block)})
    with TimingStat(timing, stat_client) as timer:
        file_lumis = api.listFileLumis(block_name=block)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                        'server_request_timestamp' : float(request_time)/1000000.0,
                        'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server()
    
    print("Found %s lumis for files in block %s" % (len(file_lumis), block))
