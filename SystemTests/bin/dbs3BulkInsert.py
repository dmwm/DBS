#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys
import time

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_WRITER_URL", "https://cmsweb.cern.ch/dbs/int/global/DBSWriter/")}

api = create_api('DbsApi', config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

# check if running a stress or integration test (Means collect statistics or not)
try:
    named_pipe = payload_handler.payload['workflow']['NamedPipe']
except KeyError:
    stat_client = None
else:
    stat_client = StatsPipeClient(named_pipe)

block_dump = payload_handler.payload['workflow']['DBS']

injection_repetition_rate = payload_handler.payload['workflow']['InjectionRepetitionRate']

## insert block to DBS using bulk block insertion
timing = {'stats':{'query' : 'insertBulkBlock', 'api' : 'insertBulkBlock'}}
for block in block_dump:
    with TimingStat(timing, stat_client) as timer:
        ds_type = api.insertBulkBlock(block)

    request_processing_time, request_time = api.requestTimingInfo
    timer.update_stats({'server_request_timing' : float(request_processing_time)/1000000.0,
                        'server_request_timestamp' : float(request_time)/1000000.0,
                        'request_content_length' : int(api.requestContentLength)})
    if stat_client:
        timer.stat_to_server()

    print("Inserted block %s" % (block['block']['block_name']))

    if injection_repetition_rate != -1:
        time.sleep(injection_repetition_rate)

p = payload_handler.clone_payload()
payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
