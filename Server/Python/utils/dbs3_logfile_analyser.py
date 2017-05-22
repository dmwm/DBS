#!/usr/bin/env python

from optparse import OptionParser
import json, os, re, sys, time
import numpy

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input JSON")

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.json (mandatory)\n"""

    if not (options.input):
        parser.print_help()
        parser.error(error_msg)

    return options

if __name__ == '__main__':
    options = get_command_line_options(os.path.basename(__file__), sys.argv)

    with open(options.input, 'r') as f:
        log_entries = json.load(f)
        data = []

        for log_entry in log_entries:
            if log_entry['request'].find('bulkblocks')!=-1:
                request_time = time.strptime(log_entry['time'], '%d/%b/%Y:%H:%M:%S')
                data.append(time.mktime(request_time))

    histo, bin_edges = numpy.histogram(data, int(max(data))-int(min(data)))
    print("Mean: %s requests/s" % (numpy.mean(histo)))
    print("Median: %s requests/s" % (numpy.median(histo)))
    print("Standard deviation: %s requests/s" %(numpy.std(histo)))
    import matplotlib.pyplot as plt
    plt.bar(bin_edges[:-1], histo, width = 1)
    plt.xlim(min(bin_edges), max(bin_edges))
    plt.show()
