from optparse import OptionParser

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage='%s options' % executable_name)
    parser.add_option('-i', '--in', type='string', dest='input', help='input payload')
    parser.add_option('-o', '--out', type='string', dest='output', help='output payload')
    options, args = parser.parse_args()
    if options.input:
        options.output or parser.print_help()
        parser.error('You need to provide following options, --in=input_payload.json and --out=output_payload.json')
    return options


if __name__ == '__main__':
    import sys
    print get_command_line_options(__name__, sys.argv)
