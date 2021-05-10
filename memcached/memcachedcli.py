import argparse
import time
from pymemcache.client.base import Client

validDic = {}
op_type = ['INSERT', 'READ', 'UPDATE', 'SCAN', 'DELETE']
endserver = False

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
    elif op == 'SCAN':
        print('Not support scan operation')
    elif op == 'DELETE':
        client.delete(key)
        if hasValidation:
            del validDic[key]
    # print(op, ':', key, value)

# YCSB load section
def ycsb_load(load_trace, client, hasValidation):
    insert_cnt = 0
    with open(load_trace) as load_file:
        for line in load_file:
            parse_line(line, client, hasValidation)
            insert_cnt += 1
    print("insert {} entries".format(insert_cnt))

def ycsb_run(run_trace, client, hasValidation, target):
    op_cnt = 0
    last_throttle_time = time.time()
    with open(run_trace) as run_file:
        for line in run_file:
            parse_line(line, client, hasValidation)
            op_cnt += 1
            if target > 0 and op_cnt % target == 0:
                cur_time = time.time()
                if cur_time - last_throttle_time > 1.00:
                    print("[warn] not satisfy time throttling", last_throttle_time, cur_time)
                else:
                    # print("[info] sleep ", cur_time - last_throttle_time, "to enable throttling")
                    time.sleep(cur_time - last_throttle_time)
                last_throttle_time = cur_time

    print("execute {} entries".format(op_cnt))

def main(args):
    global endserver
    port = args.port
    tracename = args.trace
    hasValidation = args.validate
    target = int(args.target)
    endserver = (args.endserver)

    server_addr = 'localhost:' + port
    client = Client(server_addr, default_noreply=True)

    ycsb_load(tracename + '.load', client, hasValidation)
    ycsb_run(tracename + '.run', client, hasValidation, target)
    if endserver:
        client.endserver()
    else:
        client.quit()

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
    ap.add_argument('--target', required=False,
                    default='0',
                    help='Target ops/sec')
    ap.add_argument('--endserver', action='store_true',
                    help='Quit memcached server after test finishes (require customed memcached support)')

    return ap

if __name__ == '__main__':
    main(argparser().parse_args())

