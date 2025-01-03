#!/bin/sh

docker exec -it valkey_fdw-valkey-1-1 valkey-cli --cluster create valkey-1:6379 valkey-2:6379 valkey-3:6379 --cluster-replicas 0 --cluster-yes
