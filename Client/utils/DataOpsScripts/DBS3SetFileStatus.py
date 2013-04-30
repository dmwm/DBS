#!/usr/bin/env python
"""
_DBS3InvalidateFiles_ 

Command line tool to invalidate files.

Give the file list, block name, the new status and DBS Instance url (writer), it will
set the new status.
"""
import sys
from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

def isFileValid(dbsApi, files=[], blocks=[], fstatus=0):
    #Return dictionary that has a list of invalid files' LFNs and a list of valida files' LFN.
    invalidfilelst = []
    validfilelst = []
    if files:
        for f in files:
            rslt = dbsApi.listFiles(logical_file_name=f, details=1)
            if rslt[0].is_file_valid == fstatus :
                invalidfilelst.append(f)
            else:
                validfilelst.append(f)
    #
    for block in blocks:
        rslt = dbsApi.listFiles(block_name=block, details=1)
        for r in rslt:
            if r.is_file_valid == fstatus :
                invalidfilelst.append(r.logical_file_name)
            else:
                validfilelst.append(r.logical_file_name)
    #
    return {'validfilelst':validfilelst, 'invalidfilelst':invalidfilelst}

def listFileChildren(dbsApi, files=[]):
    for cf in dbsApi.listFileChildren(logical_file_names=files):
        print('Found children file %s' % (cf['child_logical_file_name']))
        yield cf['child_logical_file_name']

def listBlockChildren(dbsApi, block=None):
    for cb in dbsApi.listBlockChildren(block_name=block):
        print('Found children block %s' % (cb['block_name']))
        yield cb['block_name']

def isChildrenValid(dbsApi, files=[], blocks=[], pstatus=0):
    allfiles, child = list(), files
    allblocks, childb = list(), blocks
    while child:
        c = child.pop()
        allfiles.append(c)
        child.extend(listFileChildren(dbsApi, files=files))
    
    while childb :
        b = childb.pop()
        allblocks.append(b)
        childb.extend(listBlockchildren(dbsApi, b))
    
    return isFileValid(files=allfiles, block=allblocks, fstatus=pstatus)

def updateFileStatus(dbsApi, status, recursive, files=[], block=None):
    lost = 0
    if status == "invalid":
        fstatus = 0
    elif status == "valid":
        fstatus = 1
    elif status == "lost":
        fstatus = 0
        lost = 1
    else: 
        print "invalid file status from user. DBS cannot set file status to be %s" %status
        sys.exit(1)
    try:
        if recursive:
            flst = isChildrenValid(dbsApi, files=files, blocks=[block],  pstatus=fstatus )
        else:
            flst = isFileValid(dbsApi, files=files, blocks=[block], status=fstatus)

        if flst['validfilelst']:
            dbsApi.updateFileStatus(logical_file_name=flst['validfilelst'], is_file_valid=fstatus, lost=lost)
        if flst['invalidfilelst']:   
            print "cannot %sate part of files that are %s. The files are %fst" %(status, status, flst['invalidfilelst'])
            sys.exit(1)
    except Exception, ex:
        print "Caught exception %s:"%str(ex)
        sys.exit(1)

def main ():
    from optparse import OptionParser
    import os
 
    usage="""\npython DBS3SetFileStatus <options> \nOptions: \n --url=<url> \t\t\t\t Required. dbs url \n --status=<valid/invalid/lost> \t\t Required.
    status to set \n --recursive=<True/False> \t\t Required. valida/invalida down to chlidren \n --files=<file_list> \t\t\t file list to be re-validate. Use --files or --block \n --block=<block_name> \t\t\t re-validate all the files in block_name """

    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url")
    parser.add_option("-s", "--status", dest="status", help="file status to be set")
    parser.add_option("-c", "--recursive", dest="recursive", help="True means in/validate will go down to chidren. False means only validate current files.")
    parser.add_option("-f", "--files", dest="files", help="list of files to be validated/invalidated. use either --files or --block")
    parser.add_option("-b", "--block", dest="block", help="block to validate/invalidate. use either --files or --block")

    (opts, args) = parser.parse_args()
    if not (opts.url and opts.status and opts.recursive and (opts.files or opts.block)):
        print usage
        sys.exit(1)

    print opts
    proxy=os.environ.get('SOCKS5_PROXY')
    dbsApi = DbsApi(url=opts.url, proxy=proxy)
    try:
        if opts.files or opts.block:
            if opts.files:
                files=opts.files.split(",")
            else: files = []
            updateFileStatus(dbsApi, opts.status, opts.recursive, files=files, block=opts.block)
    except Exception, ex:
        print "Caught exception %s:"%str(ex)
        sys.exit(1)

    print "All done"
    sys.exit(0)

if __name__ == "__main__":
  main()

