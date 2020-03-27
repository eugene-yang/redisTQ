from redis import Redis

def _redis_lists(task_name):
    return { s: f"{task_name}/{s}" for s in ['todo_queue', 'running_set', 'done_set', 'fail_set'] }

def _redis_db(conf):
    if hasattr(conf, '__dict__'):
        conf = vars(conf)
    return Redis(host=conf['host'], port=conf['port'], db=conf['db'])
