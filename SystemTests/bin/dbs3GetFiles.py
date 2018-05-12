#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL", "https://cmsweb.cern.ch:8443/dbs/int/global/DBSReader/")}

api = create_api('DbsApi', config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

named_pipe = payload_handler.payload['workflow']['NamedPipe']

stat_client = StatsPipeClient(named_pipe)

initial = payload_handler.payload['workflow']['dataset']

timing = {'stats':{'api':'listFiles', 'query':initial}}

## last step (list all files in DBS3 below the 'initial' root)
with TimingStat(timing, stat_client) as timer:
  files = api.listFiles(dataset=initial, detail=True)

timer.update_stats({'server_request_timing' : float(api.request_processing_time)/1000000.0,
                    'server_request_timestamp' : (api.request_time),
                    'request_content_length' : api.content_length})

timer.stat_to_server()

print("Found %s files"  %(len(files)))

for this_file, interval in zip(files, increase_interval(0.0, 0.1)):
  p = payload_handler.clone_payload()
  p['workflow']['logical_file_name'] = this_file['logical_file_name']
  del p['workflow']['dataset']
  #p['workflow']['Intervals']['getFileParents'] += interval
  payload_handler.append_payload(p)
 
payload_handler.save_payload(options.output)
