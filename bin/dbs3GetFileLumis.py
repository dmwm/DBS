#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

stat_client = StatsPipeClient("/tmp/dbs3fifo")

initial = payload_handler.payload['workflow']['logical_file_name']

timing = {'stats':{'api':'listFileLumis', 'query':initial}}

with TimingStat(timing, stat_client) as timer:
    file_lumis = api.listFileLumis(logical_file_name=initial)
    #print "logical_file_name (lumis): %s" % (initial)

timer.update_stats({'server_request_timing' : float(api.request_processing_time)/1000000.0,
                    'server_request_timestamp' : (api.request_time),
                    'request_content_length' : api.content_length})

timer.stat_to_server()

print "Found %s file lumis" % (len(file_lumis)) 
    
#p = payload_handler.clone_payload()
#p['workflow']['file_lumis'] = file_lumis
#payload_handler.append_payload(p)
#payload_handler.save_payload(options.output)
