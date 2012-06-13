#!/usr/bin/env python
from LifeCycleTools.APIFactory import create_api
from LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTools.Timing import TimingStat
from LifeCycleTools.OptParser import get_command_line_options

import os
import sys
import json

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

initial = payload_handler.payload['workflow']['InitialRequest']
print "Initial request string: %s" % (initial)

## first step (list all datasets in DBS3 below the 'initial' root)

timing = {'stats':{'exe':os.path.basename(__file__), 'query':initial}}

with TimingStat(timing) as timer:
    datasets = api.listDatasets(dataset=initial)

timer.update_payload({'server_request_timing' : float(api.request_processing_time)/1000000.0,
                      'server_request_timestamp' : (api.request_time),
                      'request_content_length' : api.content_length})
timer.stat_to_file(options.output.replace(".out",".stat"))

print "Found %s datasets" % (len(datasets))

for dataset, interval in zip(datasets, increase_interval(start=0.0, step=0.2)):
  p = payload_handler.clone_payload()
  p['workflow']['dataset'] = dataset['dataset']
  #p['workflow']['Intervals']['getPrimaryDatasetType'] += interval
  p['workflow']['Intervals']['CrabWorkflow'] += interval
  payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
