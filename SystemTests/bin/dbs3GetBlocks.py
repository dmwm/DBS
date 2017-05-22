#!/usr/bin/env python

from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL", "https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi', config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

named_pipe = payload_handler.payload['workflow']['NamedPipe']

stat_client = StatsPipeClient(named_pipe)

initial = payload_handler.payload['workflow']['dataset']
print("Dataset name: %s" % (initial))

timing = {'stats':{'api':'listBlocks', 'query':str(initial)}}

## next step (list all blocks in DBS3 below the 'initial' root)
with TimingStat(timing, stat_client):
  blocks = api.listBlocks(dataset=initial)

timer.stat_to_timer()

print("Found %s blocks" % (len(blocks)))

for block in blocks:
  p = payload_handler.clone_payload()
  p['workflow']['block_name'] = block['block_name']
  payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
