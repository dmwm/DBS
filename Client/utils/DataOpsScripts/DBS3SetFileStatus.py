#!/usr/bin/env python
"""
_DBS3InvalidateFiles_

Command line tool to invalidate files.

Give the file list, block name, the new status and DBS Instance url (writer), it will
set the new status.
"""
from optparse import OptionParser
import logging
import os
import sys

from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

def isFileValid(files=[], blocks=[], fstatus=0):
    #Return dictionary that has a list of invalid files' LFNs and a list of valid files' LFN.
    invalidfilelst = []
    validfilelst = []

    for f in files:
        rslt = dbsApi.listFiles(logical_file_name=f, detail=True)
        try:
            if rslt[0]['is_file_valid'] == fstatus :
                invalidfilelst.append(f)
            else:
                validfilelst.append(f)
        except IndexError:
            logging.error('The file %s does not exists in DBS. Please, check your input!' % (f))
            sys.exit(1)

    for block in blocks:
        rslt = dbsApi.listFiles(block_name=block, detail=True)
        for r in rslt:
            if r['is_file_valid'] == fstatus :
                invalidfilelst.append(r['logical_file_name'])
            else:
                validfilelst.append(r['logical_file_name'])

    return {'validfilelst':validfilelst, 'invalidfilelst':invalidfilelst}

def listFileChildren(files=[]):
    for cf in dbsApi.listFileChildren(logical_file_name=files):
        logging.debug('Found children file %s' % (cf['child_logical_file_name']))
        yield cf['child_logical_file_name']

def listBlockChildren(blocks=[]):
    for cb in dbsApi.listBlockChildren(block_name=block):
        logging.debug('Found children block %s' % (cb['block_name']))
        yield cb['block_name']

def isChildrenValid(files=[], blocks=[], pstatus=0):
    allfiles, child = list(), files
    allblocks, childb = list(), blocks
    while child:
        c = child.pop()
        allfiles.append(c)
        child.extend(listFileChildren(files=c))

    while childb :
        b = childb.pop()
        allblocks.append(b)
        childb.extend(listBlockchildren(b))

    return isFileValid(files=allfiles, blocks=allblocks, fstatus=pstatus)

def updateFileStatus(status, recursive, files=[], blocks=[]):
    #import pdb
    #pdb.set_trace()
    flst={}
    lost = 0
    if status == "invalid":
        fstatus = 0
    elif status == "valid":
        fstatus = 1
    elif status == "lost":
        fstatus = 0
        lost = 1
    else:
        logging.error("invalid file status from user. DBS cannot set file status to be %s" % status)
        sys.exit(1)

    if recursive in ['True','true', '1', 'y', 'yes', 'yeah', 'yup', 'certainly']:
        flst = isChildrenValid(files=files, blocks=blocks,  pstatus=fstatus )
    else:
        flst = isFileValid(files=files, blocks=blocks, fstatus=fstatus)

    if flst['validfilelst']:
        logging.debug('updateFileStatus: lfn:%s, is_file_valid:%s, lost:%s' % (flst['validfilelst'], fstatus, lost))
        dbsApi.updateFileStatus(logical_file_name=flst['validfilelst'], is_file_valid=fstatus, lost=lost)
        #for f in flst['validfilelst']:
            #dbsApi.updateFileStatus(logical_file_name=f, is_file_valid=fstatus, lost=lost)
    if flst['invalidfilelst']:
        logging.error("cannot %s some of files that are already %s. These files are %s" % (status, status,
                                                                                           flst['invalidfilelst']))
        sys.exit(1)

def main():
    usage="%prog <options>"

    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url (Required)", metavar="<url>")
    parser.add_option("-s", "--status", dest="status", help="File status to be set (Required)",
                      metavar="<valid/invalid/lost>")
    parser.add_option("-r", "--recursive", dest="recursive",
                      help="True means (in)validate will go down to chidren. False means only validate given files.\
                      (Required)",
                      metavar="<True/False>")
    parser.add_option("-f", "--files", dest="files",
                      help="List of files to be validated/invalidated. Can be either a file containg lfns or a \
                      comma separated list of lfn's. Use either --files or --block",
                      metavar="<lfn1,..,lfnx or filename>")
    parser.add_option("-b", "--block", dest="blocks", help="Blocks to validate/invalidate. \
                      use either --files or --block", metavar="<block_name>")
    parser.add_option("-p", "--proxy", dest="proxy", help="Use Socks5 proxy to connect to server",
                      metavar="socks5://127.0.0.1:1234")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Increase verbosity")

    #set default values
    parser.set_defaults(files=[])
    parser.set_defaults(blocks=[])

    (opts, args) = parser.parse_args()
    if not (opts.url and opts.status and opts.recursive and (opts.files or opts.blocks)):
        parser.print_help()
        parser.error('Mandatory options are --block or --file, --status, --url and --recursive')

    log_level = logging.DEBUG if opts.verbose else logging.INFO
    logging.basicConfig(format='%(message)s', level=log_level)

    global dbsApi
    dbsApi = DbsApi(url=opts.url, proxy=opts.proxy)
    files = []
    if opts.files:
        try:
            with open(opts.files, 'r') as f:
                files = [lfn.strip() for lfn in f]
        except IOError:
            opts.files = opts.files.strip(",").strip()
            for f in opts.files.split(","):
                files.append(f.strip())
        finally:
            blocks = []

    elif opts.blocks:
        blocks = opts.blocks.split(",")
    updateFileStatus(opts.status, opts.recursive, files=files, blocks=blocks)

    logging.info("All done")

if __name__ == "__main__":
  main()

