#!/usr/bin/env python

from optparse import OptionParser
import json, os, re, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input DAS Logfile")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output JSON")

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.txt (mandatory), --out=output.json (optional)\n"""
    
    if not options.input:
        parser.print_help()
        parser.error(error_msg)

    return options

def replace_values(log_entry):
    if log_entry["user"] == "-":
        log_entry["user"] = None

    log_entry["status"] = int(log_entry["status"])
        
    log_entry["size"] = 0 if log_entry["size"] == "-" else int(log_entry["size"])

    if log_entry["referer"] == "-":
        log_entry["referer"] = None

    return log_entry

### using regular expressions from http://www.seehuhn.de/blog/52 by Jochen Voss (Creative Common License)
log_parts = [r'(?P<host>\S+)',                   # host %h
             r'\S+',                             # indent %l (unused)
             r'(?P<user>\S+)',                   # user %u
             r'\[(?P<time>.+)\]',                # time %t
             r'"(?P<request>.+)"',               # request "%r"
             r'(?P<status>[0-9]+)',              # status %>s
             r'(?P<size>\S+)',                   # size %b (careful, can be '-')
             r'"(?P<referer>.*)"',               # referer "%{Referer}i"
             r'"(?P<agent>.*)"',                 # user agent "%{User-agent}i"
             ]

log_pattern = re.compile(r'\s+'.join(log_parts)+r'\s*\Z')

options = get_command_line_options(os.path.basename(__file__), sys.argv)

log_entries = []

with open(options.input, 'r') as f:
    for line in f:
        match_obj = log_pattern.match(line)
        log_entries.append(replace_values(match_obj.groupdict()))

if options.output:
    with open(options.output, 'w') as f:
        json.dump(log_entries, f)
else:
    print(log_entries)    
    
    
