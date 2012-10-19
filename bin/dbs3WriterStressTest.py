#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options

import sys

options = get_command_line_options(__name__, sys.argv)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

initial_payload = payload_handler.get_payload()

for workflow in xrange(initial_payload['workflow']['NumberOfWorkflows']):
    p = payload_handler.clone_payload()
    print "Create new payload for workflow: %s" % (workflow)
    payload_handler.append_payload(p)
    payload_handler.save_payload(options.output)
