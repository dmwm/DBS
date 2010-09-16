export PATH=$MYSQL_ROOT/bin:$PATH
#update pd to real pd
mysql --port=$MYSQL_PORT --socket=$MYSQL_SOCK -uroot -ppd < create-mysql-schema.sql
mysql --port=$MYSQL_PORT --socket=$MYSQL_SOCK -uroot -ppd < initialize-mysql.sql
mysql --port=$MYSQL_PORT --socket=$MYSQL_SOCK -uroot -ppd < create-seqTb-mysql.sql
mysql --port=$MYSQL_PORT --socket=$MYSQL_SOCK -uroot -ppd < initialize-mysql-seq.sql

