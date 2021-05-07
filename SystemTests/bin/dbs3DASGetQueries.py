#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, split_list
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import json
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

data_file = payload_handler.payload['workflow']['DASQueryDataFile']
num_of_repetitions = payload_handler.payload['workflow']['NRepetitions']
num_of_threads = payload_handler.payload['workflow']['NThreads']

data_file = os.path.join(os.getenv('DBS3_LIFECYCLE_ROOT'), 'data', data_file)

with open(data_file, 'r') as f:
    queries = json.load(f)

events = ['DASAccess' for _ in range(num_of_repetitions)]
for chunk in split_list(queries, num_of_threads):
    p = payload_handler.clone_payload()
    p['workflow']['DASQueries'] = chunk
    p['workflow']['Events'].extend(events)
    payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
