#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys
import tempfile

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL", "https://cmsweb.cern.ch:8443/dbs/int/global/DBSReader/")}

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

## list parent_files and file_lumis for all the files
for this_file in files:
    logical_file_name = this_file.get("logical_file_name")
    timing.get('stats').update({'api' : 'listFileParents','query' : str(logical_file_name)})
    with TimingStat(timing, stat_client) as timer:
        parent_files = api.listFileParents(logical_file_name=logical_file_name)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                        'server_request_timestamp' : float(request_time)/1000000.0,
                        'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server()
    
    print("Found %s parents for file %s" % (len(parent_files), logical_file_name))

    timing.get('stats').update({'api' : 'listFileLumis','query' : str(logical_file_name)})
    with TimingStat(timing, stat_client) as timer:
        file_lumis = api.listFileLumis(logical_file_name=logical_file_name)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                        'server_request_timestamp' : float(request_time)/1000000.0,
                        'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server()
    
    print("Found %s lumis for file %s" % (len(file_lumis), logical_file_name))
                                          
