"""
build functions for
1. binding the workers with redis listener(with multiprocessing) for slave
2. dispatcher for master node along with good tqdm
3. general function that wrap around master and slave
3. util function for argparse additional redis settings
4. util main script for listening to certain set of task
"""

import pickle, gzip
import time

import socket
import os
from tqdm import tqdm

from multiprocessing import Process

from .connection import _redis_lists, _redis_db

def confirm(str):
    return input(str).lower() in ['yes', 'y']

def _worker_wrap(redisconf, worker, args):
    # wrapper around actual worker that receiev tasks from redis
    rdb = redisconf['rdb']

    hostname = socket.gethostname().split(".")[0]
    fail_count = 0
    current = None
    
    try:
        while fail_count < redisconf['fail_tolerance'] :
            rawtask = rdb.lpop( redisconf['todo_queue'] )
            if rawtask:
                task = pickle.loads(gzip.decompress(rawtask))
                tag =  f"{hostname}/{task[0]}" # task[0] should always be the tag
                try: 
                    rdb.sadd( redisconf['running_set'], rawtask )
                    current = rawtask
                    worker( *args, task )
                    current = None
                    rdb.sadd( redisconf['done_set'], tag )
                    fail_count = 0
                except:
                    rdb.sadd( redisconf['fail_set'], rawtask )
                    fail_count += 1
                finally:
                    rdb.srem( redisconf['running_set'], rawtask )
            else:
                time.sleep( redisconf['sleep'] )
        else:
            # too many fails
            print("[Too many fails in a row!]")

    except KeyboardInterrupt:
        print("[Keyboard Break!]")
    finally:
        if current is not None:
            rdb.sadd( redisconf['fail_set'], current )
            rdb.srem( redisconf['running_set'], current )


def start_slave_process(cliargs, task_name, worker_func, bind_args):
    # starting multiprocessing workers
    redisconf = {
        'rdb': _redis_db(cliargs),
        # redis setting
        **{ s:vars(cliargs)[s] for s in ['sleep', 'fail_tolerance'] },
        # redis set & queues
        **_redis_lists(task_name),
    }

    print(f"[Starting multiprocessing with {cliargs.worker} workers]")
    ps = [ Process(target=_worker_wrap, args=(redisconf, worker_func, bind_args))
           for _ in range(cliargs.worker) ]
    [ p.start() for p in ps ]
    [ p.join() for p in ps ]

def dispatch_tasks(cliargs, task_name, tasks):
    rdb = _redis_db(cliargs)
    todo = _redis_lists(task_name)['todo_queue']
    # TODO: add warnning for writing to existing queue
    if rdb.llen(todo) > 0:
        print(f"[[[[ WARNING: {todo} is not Empty ]]]]")
        if not confirm("overwrite? [y/n]"):
            exit(1)
    rdb.delete(todo)
    print("[Dispatching...]")
    for tk in tqdm(tasks):
        rdb.rpush(todo, gzip.compress( pickle.dumps(tk) ) )

def add_redis_arguments(parser):
    # could pass subparser in
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=2232)
    parser.add_argument('--db', type=int, default=0)

    gp = parser.add_mutually_exclusive_group(required=True)
    gp.add_argument('--master', action='store_true')
    gp.add_argument('--slave', action='store_true')

    parser.add_argument('--worker', type=int, default=4)
    parser.add_argument('--sleep', type=float, default=5.)
    parser.add_argument('--fail_tolerance', type=int, default=3)

    parser.set_defaults(redis=True)




