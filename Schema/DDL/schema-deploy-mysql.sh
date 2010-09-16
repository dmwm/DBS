#!/bin/bash
#export PATH=$MYSQL_ROOT/bin:$PATH
#update pd to real pd
mysql --port=$MYSQL_PORT --socket=$MYSQL_SOCK -uroot -ppd << ENDOFMYSQL 
source create-mysql-schema.sql
source initialize-mysql.sql
source create-seqTb-mysql.sql
source initialize-mysql-seq.sql

