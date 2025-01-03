#!/bin/sh

IP=$(docker exec -it valkey_fdw-postgres-1 ping -c 1 valkey-1  | grep PING | awk '{print $3}' | sed -e 's/(//' -e 's/)://' )
sed -e "s%@VALKEY_IP@%${IP}%g" init.sql > init.sql.tmp
export PGPASSWORD=password
psql -h 127.0.0.1 -U postgres  -f init.sql.tmp
rm init.sql.tmp
