#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options

import os
import sys

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

initial = payload_handler.payload['workflow']['dataset']

timing = {'stats':{'exe':os.path.basename(__file__), 'query':initial}}

## last step (list all files in DBS3 below the 'initial' root)
with TimingStat(timing) as timer:
  files = api.listFiles(dataset=initial, detail=True)

timer.update_payload({'server_request_timing' : float(api.request_processing_time)/1000000.0,
                      'server_request_timestamp' : (api.request_time),
                      'request_content_length' : api.content_length})

timer.stat_to_file(options.output.replace(".out",".stat"))

print "Found %s files"  %(len(files))

for this_file, interval in zip(files, increase_interval(0.0, 0.1)):
  p = payload_handler.clone_payload()
  p['workflow']['logical_file_name'] = this_file['logical_file_name']
  del p['workflow']['dataset']
  #p['workflow']['Intervals']['getFileParents'] += interval
  payload_handler.append_payload(p)
 
payload_handler.save_payload(options.output)
