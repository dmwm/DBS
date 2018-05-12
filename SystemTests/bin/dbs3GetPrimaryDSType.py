#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTools.APIFactory import create_api
from LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTools.Timing import TimingStat
from LifeCycleTools.OptParser import get_command_line_options
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

timing = {'stats':{'api':'listPrimaryDSTypes', 'query':initial}}

with TimingStat(timing, stat_client) as timer:
    ds_type = api.listPrimaryDSTypes(dataset=initial)[0]

timer.update_stats({'server_request_timing' : float(api.request_processing_time)/1000000.0,
                    'server_request_timestamp' : (api.request_time),
                    'request_content_length' : api.content_length})

timer.stat_to_server()

print("data type: %s" % (ds_type['data_type']))

p = payload_handler.clone_payload()
#p['workflow']['data_type'] = ds_type['data_type']
payload_handler.append_payload(p)
payload_handler.save_payload(options.output)
