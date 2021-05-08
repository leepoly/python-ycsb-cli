python-ycsb-cli
===
A python implementation of YCSB client interface.

### YCSB Generate trace

`./bin/ycsb load basic -s -P workloads/workloada > ycsb_load.txt`
`./bin/ycsb run basic -s -P workloads/workloada > ycsb_run.txt`

### Memcached

Install pymemcache package by:

`python3 -m pip install pymemcache`

Setup memcached:

`sudo apt install memcached`

`./memcached -t 4 -m 64m`

Connect and test:

`python3 memcached/memcachedcli.py`