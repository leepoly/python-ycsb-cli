from pymemcache.client.base import Client
import os, sys

hasValidation = True
validDic = {}

op_list = ['INSERT', 'READ', 'UPDATE', 'SCAN']

def parse_line(line):
    found_op = 'None'
    for op in op_list:
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
def ycsb_load(load_trace):
    insert_cnt = 0
    with open(load_trace) as load_file:
        for line in load_file:
            parse_line(line)
            insert_cnt += 1
    print("insert {} entries".format(insert_cnt))

def ycsb_run(run_trace):
    check_cnt = 0
    with open(run_trace) as run_file:
        for line in run_file:
            parse_line(line)
            check_cnt += 1
    print("execute {} entries".format(check_cnt))


port = '11211'
if len(sys.argv) > 1:
    port = sys.argv[1]
server_addr = 'localhost:' + port
client = Client(server_addr)

ycsb_load('../workload/ycsb_load.txt')
ycsb_run('../workload/ycsb_run.txt')
