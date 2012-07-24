#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys
import json

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

stat_client = StatsPipeClient("/tmp/dbs3fifo")

initial = payload_handler.payload['workflow']['InitialRequest']
print "Initial request string: %s" % (initial)

## first step (list all datasets in DBS3 below the 'initial' root)

timing = {'stats':{'api':'listDatasets', 'query':str(initial)}}

with TimingStat(timing, stat_client) as timer:
    datasets = api.listDatasets(dataset=initial)
    
request_processing_time, request_time = api.requestTimingInfo
timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                    'server_request_timestamp' : float(request_time)/1000000.0,
                    'request_content_length' : api.requestContentLength})

timer.stat_to_server()

print "Found %s datasets" % (len(datasets))

for dataset, interval in zip(datasets, increase_interval(start=0.0, step=0.2)):
  p = payload_handler.clone_payload()
  p['workflow']['dataset'] = dataset['dataset']
  #p['workflow']['Intervals']['getPrimaryDatasetType'] += interval
  p['workflow']['Intervals']['CrabWorkflow'] += interval
  payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
