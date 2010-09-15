#!/bin/bash
python ./generate-schema-deployable.py
#login to db
sqlplus YOUR_OWNER_ACCOUNT@YOUR_DB/YOUR_PD <<ENDOFSQL
@oracle-deployable.sql
@auto_inc_trigs.sql
@initialize-template.sql
exit 
ENDOFSQL
