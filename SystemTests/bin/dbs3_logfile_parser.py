#!/usr/bin/env python

from optparse import OptionParser
import json, os, re, sys

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input DBS3 Logfile")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output JSON")

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.txt (mandatory), --out=output.json (optional)\n"""
    
    if not options.input:
        parser.print_help()
        parser.error(error_msg)

    return options

def replace_values(log_entry):
    if log_entry["dn"] == "":
        log_entry["dn"] = None
    if log_entry["dontknow"] == "":
        log_entry["dontknow"] = None

    log_entry["status"] = int(log_entry["status"])
        
    log_entry["data_in"] = 0 if log_entry["data_in"] == "-" else int(log_entry["data_in"])
    log_entry["data_out"] = 0 if log_entry["data_out"] == "-" else int(log_entry["data_out"])
    log_entry["data_us"] = 0 if log_entry["data_us"] == "-" else int(log_entry["data_us"])

    if log_entry["referer"] == "-":
        log_entry["referer"] = None

    return log_entry

log_parts = [r'^INFO:cherrypy.access:\[(?P<time>\S+)\]',                                        # time
             r'(?P<host>\S+)',                                                                  # host
             r'(?P<ip>\S+)',                                                                    # ip
             r'"(?P<request>.+)"',                                                              # request
             r'(?P<status>[0-9]+)',                                                             # status
             r'\[data:\s(?P<data_in>\S+)\sin\s(?P<data_out>\S+)\sout\s(?P<data_us>\S+)\sus\s]', # data
             r'\[auth:\sOK\s"(?P<dn>.*?)"\s"(?P<dontknow>.*?)"\s\]',                            # auth data
             r'\[ref:\s"(?P<referer>.*?)"\s"(?P<agent>.*?)"\s\]'                                # referer and agent
             ]

log_pattern = re.compile(r'\s+'.join(log_parts)+r'\s*\Z')

options = get_command_line_options(os.path.basename(__file__), sys.argv)

log_entries = []

with open(options.input, 'r') as f:
    for line in f:
        match_obj = log_pattern.match(line)
        try:
            log_entries.append(replace_values(match_obj.groupdict()))
        except AttributeError:
            print("The following line does not match the pattern:\n %s" % (line.strip()))
            pass

if options.output:
    with open(options.output, 'w') as f:
        json.dump(log_entries, f)
else:
    print(log_entries)
