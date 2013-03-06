#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import json
import os
import sys
import tempfile

options = get_command_line_options(__name__, sys.argv)

config = {'url':os.environ.get("DBS_READER_URL","https://cmsweb.cern.ch/dbs/int/global/DBSReader/")}

api = create_api('DbsApi',config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

named_pipe = payload_handler.payload['workflow']['NamedPipe']

stat_client = StatsPipeClient(named_pipe)

data_file = payload_handler.payload['workflow']['DASQueryDataFile']
num_of_repetitions = payload_handler.payload['workflow']['NRepetitions']

data_file = os.path.join(os.getenv('DBS3_LIFECYCLE_ROOT'), 'data', data_file)

with open(data_file, 'r') as f:
    queries = json.load(f)
    events = ['DASAccess' for _ in xrange(num_of_repetitions)]
    for query in queries:
        p = payload_handler.clone_payload()
        p['workflow']['DASQuery'] = query
        p['workflow']['Events'].extend(events)
        payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
