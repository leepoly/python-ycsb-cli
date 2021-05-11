import argparse
import time
import os, sys
import asyncio
from subprocess import Popen
from signal import *
from pymemcache.client.base import Client

validDic = {}
op_type = ['INSERT', 'READ', 'UPDATE', 'SCAN', 'DELETE']
endserver = False
client = None
zsim_proc = None

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
    print("[client] insert {} entries".format(insert_cnt))

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

    print("[client] execute {} entries".format(op_cnt))

# async def get_mapped_port(zsimruncfg):
def get_mapped_port(zsimruncfg):
    filepath = os.path.join(os.path.dirname(zsimruncfg), 'p0', 'portlist') # we assume memcached is executed in the first process p0
    print(filepath)
    while not os.path.exists(filepath):
        print('[client] wait 10s')
        # await asyncio.sleep(1)
        time.sleep(10)
    real = -1
    with open(filepath) as port_list:
        for line in port_list:
            virt = line.split(' ')[0]
            if virt == '11211':
                real = line.split(' ')[1].strip()
            break
    print("[client] get real port {}!".format(real))
    return real

def remove_prev_portlist(zsimruncfg):
    filepath = os.path.join(os.path.dirname(zsimruncfg), 'p0', 'portlist') # we assume memcached is executed in the first process p0
    if os.path.exists(filepath):
        os.remove(filepath)

def sim_end(*args):
    if client is not None:
        if endserver:
            client.endserver()
        else:
            client.quit()
    # elif zsim_proc is not None:
    #     zsim_proc.terminate()
    sys.exit(0)

def main(args):
    global endserver, zsim_proc
    port = args.port
    tracename = args.trace
    hasValidation = args.validate
    target = int(args.target)
    endserver = args.endserver
    if args.zsimruncfg is not None:
        endserver = True # must quit server after simulation
        for sig in (SIGABRT, SIGILL, SIGINT, SIGSEGV, SIGTERM):
            signal(sig, sim_end)
        assert(args.zsimpath is not None)
        zsimbinpath = os.path.abspath(args.zsimpath)
        zsimrundir = os.path.dirname(args.zsimruncfg)
        cur_path = os.getcwd()
        remove_prev_portlist(args.zsimruncfg)
        os.chdir(zsimrundir)
        zsim_proc = Popen([zsimbinpath, args.zsimruncfg])
        os.chdir(cur_path)
        # port = asyncio.run(get_mapped_port(args.zsimruncfg))
        port = get_mapped_port(args.zsimruncfg)
        if port == -1:
            print("[Error] cannot get memcached mapped port")
            sim_end()

    server_addr = 'localhost:' + port
    client = Client(server_addr, default_noreply=True)

    ycsb_load(tracename + '.load', client, hasValidation)
    ycsb_run(tracename + '.run', client, hasValidation, target)
    sim_end()


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
    ap.add_argument('--zsimpath', required=False,
                    default=None,
                    help='Zsim binary path')
    ap.add_argument('--zsimruncfg', required=False,
                    default=None,
                    help='Start simulating memcached server in zsim. Set zsimpath and zsimruncfg')

    return ap

if __name__ == '__main__':
    main(argparser().parse_args())

