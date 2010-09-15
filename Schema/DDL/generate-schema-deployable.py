#!/usr/bin/env python

import shutil
import fileinput
import sys

#One has to update these SEVEN DB variables to fit your db accounts before you use this script.
schema_owner="CMS_LUM_OWNER"
reader_role_name="CMS_LUM_READER_ROLE"
writer_role_name="CMS_LUM_WRITER_ROLE"
admin_role_name="CMS_LUM_ADMIN_ROLE"
#
reader_account="CMS_LUM_READER"
writer_account="CMS_LUM_WRITER"
admin_account="CMS_LUM_ADMIN"
#
shutil.copyfile('create-oracle-schema.sql','oracle-deployable.sql')
for line in fileinput.input("oracle-deployable.sql",inplace=1):
    if "CMS_DBS3_READ_ROLE" in line:
            line=line.replace("CMS_DBS3_READ_ROLE", reader_role_name)
    if "CMS_DBS3_WRITE_ROLE" in line:
                line=line.replace("CMS_DBS3_WRITE_ROLE", writer_role_name)
    if "CMS_DBS3_ADMIN_ROLE" in line:
                line=line.replace("CMS_DBS3_ADMIN_ROLE", admin_role_name)
    if "CMS_DBS3_READER" in line:
	line=line.replace("CMS_DBS3_READER", reader_account)
    if "CMS_DBS3_WRITER" in line:
            line=line.replace("CMS_DBS3_WRITER", writer_account)
    if "CMS_DBS3_ADMIN" in line:
                line=line.replace("CMS_DBS3_ADMIN", admin_account)
    sys.stdout.write(line)
