import argparse
from tqdm import tqdm
from .connection import _redis_lists, _redis_db

def _get_tasks(rdb, exp_name):
    return set([ s.decode().split("/")[1] for s in rdb.keys(f"{exp_name}/*") ])

def _progress(rdb, args):
    import pandas as pd
    tasks = _get_tasks(rdb, args.exp_name)

    res = pd.DataFrame({
        tk: {
            l: rdb.scard(f"{args.exp_name}/{tk}/{l}") if 'set' in l else rdb.llen(f"{args.exp_name}/{tk}/{l}")
            for l in ['todo_queue', 'running_set', 'done_set', 'fail_set']
        }
        for tk in tasks
    })

    order = args.sortby if args.sortby is not None else ['todo_queue', 'done_set']
    print(res.T.sort_index().sort_values(order))


def _summary(args):
    raise NotImplementedError("Summary feature is not implemented yet.")

def _clear(rdb, args):
    for tk in args.tasks:
        if args.show_fail:
            fs = rdb.scard(f"{args.exp_name}/{tk}/fail_set")
            print(f"{tk} fails: {fs}")
        rdb.delete(f"{args.exp_name}/{tk}/fail_set")
        if args.clear_running:
            rdb.delete(f"{args.exp_name}/{tk}/running_set")
        if args.clear_done:
            rdb.delete(f"{args.exp_name}/{tk}/done_set")
    # clear the fail and running
    # not sure whether it is better to just remove the entire thing

def _recycle(rdb, args):
    tasks = args.tasks
    if args.all:
        tasks = _get_tasks(rdb, args.exp_name)

    for tk in tasks:
        l = rdb.scard(f"{args.exp_name}/{tk}/fail_set")
        if l > 0:    
            for _ in tqdm(range(l), desc=tk):
                e = rdb.spop(f"{args.exp_name}/{tk}/fail_set")
                rdb.rpush( f"{args.exp_name}/{tk}/todo_queue", e )
        if args.force:
            l = rdb.scard(f"{args.exp_name}/{tk}/running_set")
            if l > 0:
                for _ in tqdm(range(l), desc=tk):
                    e = rdb.spop(f"{args.exp_name}/{tk}/running_set")
                    rdb.rpush( f"{args.exp_name}/{tk}/todo_queue", e )
    
def _remove(rdb, args):
    for tk in args.tasks:
        for l in['todo_queue', 'running_set', 'done_set', 'fail_set']:
            rdb.delete(f"{args.exp_name}/{tk}/{l}")

def main():
    parser = argparse.ArgumentParser(prog="redisTQ")
    parser.add_argument('exp_name')

    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=2232)
    parser.add_argument('--db', type=int, default=0)
    subparsers = parser.add_subparsers()

    parser_progress = subparsers.add_parser('progress')
    parser_progress.add_argument('--sortby', type=str, nargs='+')
    parser_progress.set_defaults(func=_progress)

    parser_summary = subparsers.add_parser('summary')
    parser_summary.add_argument('--show_fail', action='store_true')
    parser_summary.set_defaults(func=_summary)

    parser_clear = subparsers.add_parser('clear')
    parser_clear.add_argument('--show_fail', action='store_true')
    parser_clear.add_argument('--clear_running', action='store_true')
    parser_clear.add_argument('--clear_done', action='store_true')
    parser_clear.add_argument('--tasks', nargs='+')
    parser_clear.set_defaults(func=_clear)
    
    parser_remove = subparsers.add_parser('remove')
    parser_remove.add_argument('--tasks', nargs='+')
    parser_remove.set_defaults(func=_remove)

    parser_recycle = subparsers.add_parser('recycle')
    parser_recycle.add_argument('--tasks', nargs='+')
    parser_recycle.add_argument('--all', action='store_true')
    parser_recycle.add_argument('--force', action='store_true')
    parser_recycle.set_defaults(func=_recycle)

    args = parser.parse_args()
    args.func(_redis_db(args), args)

if __name__ == '__main__':
    main()