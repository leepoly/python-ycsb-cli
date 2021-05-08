import argparse
import os, sys
from pymemcache.client.base import Client

validDic = {}

op_type = ['INSERT', 'READ', 'UPDATE', 'SCAN']

def parse_line(line, client, hasValidation):
    found_op = 'None'
    for op in op_type:
        if line.startswith(op):
            found_op = op
            break
    if found_op == 'None':
        return
    arr = line.split(' ')
    assert(len(arr) >= 3)
    key = '-'.join((arr[1], arr[2]))
    value = line[len(op) + len(arr[1]) + len(arr[2]) + 3 : ] # + 3 for 3 spaces
    if op == 'INSERT' or op == 'UPDATE':
        if hasValidation:
            validDic[key] = value
        client.set(key, value)
    elif op == 'READ':
        cli_value = client.get(key)
        if hasValidation:
            cli_value_decode = cli_value.decode()
            assert(cli_value_decode == validDic[key])
    # print(op, ':', key, value)

# YCSB load section
def ycsb_load(load_trace, client, hasValidation):
    insert_cnt = 0
    with open(load_trace) as load_file:
        for line in load_file:
            parse_line(line, client, hasValidation)
            insert_cnt += 1
    print("insert {} entries".format(insert_cnt))

def ycsb_run(run_trace, client, hasValidation):
    op_cnt = 0
    with open(run_trace) as run_file:
        for line in run_file:
            parse_line(line, client, hasValidation)
            op_cnt += 1
    print("execute {} entries".format(op_cnt))

def main(args):
    port = args.port
    tracename = args.trace
    hasValidation = args.validate

    server_addr = 'localhost:' + port
    client = Client(server_addr, default_noreply=True)

    ycsb_load(tracename + '.load', client, hasValidation)
    ycsb_run(tracename + '.run', client, hasValidation)
    client.endserver()

def argparser():
    ''' Argument parser. '''
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', required=False,
                    default='11211',
                    help='Port ID. Memcached default: 11211')
    ap.add_argument('--trace', required=False,
                    default='workload/workloada',
                    help='Trace name. Make sure the existance of workloada.load and workloada.run respectively.')
    ap.add_argument('--validate', action='store_true',
                    help='Validate the correctness of memcached.')

    return ap

if __name__ == '__main__':
    main(argparser().parse_args())

