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

stat_output_dir = os.path.dirname(options.input)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

stat_client = StatsPipeClient("/tmp/dbs3fifo")

initial = payload_handler.payload['workflow']['dataset']

timing = {'stats':{'query' : initial}}

## list primary data type
with TimingStat(timing) as timer:
    ds_type = api.listPrimaryDSTypes(dataset=initial)[0]

request_processing_time, request_time = api.requestTimingInfo
timer.update_payload({'api' : 'listPrimaryDSTypes',
                      'server_request_timing' : float(request_processing_time)/1000000.0,
                      'server_request_timestamp' : float(request_time)/1000000.0,
                      'request_content_length' : int(api.requestContentLength)})

timer.stat_to_server(stat_client)

print "PrimaryDSType is %s" % (ds_type)

## list all files in DBS3 for a given dataset
with TimingStat(timing) as timer:
  files = api.listFiles(dataset=initial, detail=True)

request_processing_time, request_time = api.requestTimingInfo
timer.update_payload({'api' : 'listFiles',
                      'server_request_timing' : float(request_processing_time)/1000000.0,
                      'server_request_timestamp' : float(request_time)/1000000.0,
                      'request_content_length' : int(api.requestContentLength)})

timer.stat_to_server(stat_client)

print "Found %s files for dataset %s" % (len(files), initial)

## list parent_files and file_lumis for all the files
for this_file in files:
    logical_file_name = this_file.get("logical_file_name")
    with TimingStat(timing) as timer:
        parent_files = api.listFileParents(logical_file_name=logical_file_name)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_payload({'api' : 'listFileParents',
                          'query' : str(logical_file_name),
                          'server_request_timing' : float(request_processing_time)/1000000.0,
                          'server_request_timestamp' : float(request_time)/1000000.0,
                          'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server(stat_client)
    
    print "Found %s parents for file %s" % (len(parent_files), logical_file_name)

    with TimingStat(timing) as timer:
        file_lumis = api.listFileLumis(logical_file_name=logical_file_name)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_payload({'api' : 'listFileLumis',
                          'query' : str(logical_file_name),
                          'server_request_timing' : float(request_processing_time)/1000000.0,
                          'server_request_timestamp' : float(request_time)/1000000.0,
                          'request_content_length' : int(api.requestContentLength)})
    
    timer.stat_to_server(stat_client)
    
    print "Found %s lumis for file %s" % (len(file_lumis), logical_file_name)
                                          
