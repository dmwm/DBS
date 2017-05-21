#!/usr/bin/env python
"""
Script to find block names failed with block already in DBS error in cmsweb logs
To just find duplicated block names, one can use the following command in bash
grep -o -P \"(?<=Block name:\s)\S+$\" dbs-20130521.log | uniq  -d

Not yet working since log files ar not consecutive at the moment
"""

from optparse import OptionParser

import re

def get_command_line_options():
    parser = OptionParser(usage='%prog --log=<file.txt>')
    parser.add_option("-l", "--log", dest="logfile", help="CMSWEB logfile", metavar="file.txt")
    (options, args) = parser.parse_args()

    if not (options.logfile):
        parser.print_help()
        parser.error('Mandatory options are --log')

    return options, args

def find_status_code(iterator):
    while True:
        log_line = next(iterator)
        match_obj = log_pattern.match(log_line)
        try:
            match_dict = match_obj.groupdict()
        except AttributeError:
            pass
        else:
            if match_dict['request'] == 'POST /dbs/prod/global/DBSWriter/bulkblocks HTTP/1.1':
                return match_dict['status']

if __name__ == '__main__':
    options, args = get_command_line_options()

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
    block_regex = re.compile(r'^Block name: (?P<block_name>\S+)$')

    with open(options.logfile, 'r') as f:
        read_lines = (read_line.strip() for read_line in f)

        for line in read_lines:
            match_obj = block_regex.match(line)
            if match_obj:
                status_code = find_status_code(read_lines)
                if status_code!='200':
                    print(match_obj.groupdict()['block_name'])
                    print(status_code)
