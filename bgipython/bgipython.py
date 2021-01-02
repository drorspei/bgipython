"""
Cells must run in a separate thread
Api:

>>> import time
>>> time.sleep(10); print("Done!")  # immediately returns
>>> %bg
[1] sending to background
>>> print("Waiting...")
Waiting...
>>> # just waiting
Done!


1. There is always a single queue up front
2. %bg pushes a new queue on top, when existing queues finish then they're out
3. %bgwait [secs=60] waits for queue to finish or until timeout
4. %bgjobs prints what's currently running


init:
    Create a jobs queue
    Create a new thread, loop on queue
    Change ipython's run_code to send code to queue

loop in thread:
    Wait for job
    Run job
"""

from IPython.core.magic import register_line_magic
from IPython.core.magic_arguments import (argument, magic_arguments, 
                                          parse_argstring)

import linecache
from queue import Queue
import re
from threading import Thread
import time
from typing import Dict, List
from types import CodeType

ext_state = "never loaded"
ip = None
old_run_code = None

cells = []

queues: List[Queue] = []
jobs: Dict[int, List] = {}
num_queues = 0
sentinel = object()

magic_call_pattern_pattern = (
    r"^get_ipython\(\)\.run_line_magic"
    r"\((['\"]({})['\"]), (.*)$"
)
magic_call_pattern = "$^"
magic_call_repl = f"(get_ipython().run_line_magic(\\1, \\3, {id(sentinel)})[0]"

bgmagics = []
bgmagic_names = ["unload_ext"]

def register_bg_magic_funcs(func):
    """Function will be registered as magic and will be run on main thread"""
    global magic_call_pattern

    bgmagics.append(func)
    bgmagic_names.append(func.__name__)

    magic_call_pattern = re.compile(
        magic_call_pattern_pattern.format('|'.join(bgmagic_names))
    )

    return func

@register_bg_magic_funcs
def bg(_):
    """Send top jobs thread to background and create new jobs thread"""
    if ext_state != "enabled":
        return

    print(f"[{num_queues}] sending to background")
    queues.pop().put(sentinel)
    new_thread()

max_timeout_secs = 600
timeout_step_secs = 0.1

@magic_arguments()
@argument(
    "timeout",
    type=int,
    default=60,
    nargs="?",
    help="Maximum number of seconds to wait."
)
@register_bg_magic_funcs
def bgwait(arg):
    """Wait for jobs to finish, up to timeout time"""
    if ext_state != "enabled":
        return

    if not sum(map(len, jobs.values())):
        print("No background jobs")
        return

    args = parse_argstring(bgwait, arg)
    timeout: int = args.timeout
    timeout = min(timeout, max_timeout_secs)
    start = time.time()
    now = time.time()
    while now - start < timeout:
        if not sum(map(len, jobs.values())):
            return
        time.sleep(timeout_step_secs)
        now = time.time()
    print("timeout reached")

max_job_line_chars = 70

@register_bg_magic_funcs
def bgjobs(_):
    """Print running jobs"""
    if ext_state != "enabled":
        return

    # We need to slice in order to get new list instances
    # that won't change by another thread.
    partialjobs = {
        queue_num: l[:1]
        for queue_num, l in jobs.items()
    }
    for queue_num, l in partialjobs.items():
        # But list could've become empty right before we sliced.
        if len(l):
            code_obj: CodeType = l[0]
            lines = linecache.cache.get(code_obj.co_filename)
            if lines is not None:
                _, _, lines, _ = lines
                line = lines[code_obj.co_firstlineno - 1]
                if len(line) > max_job_line_chars:
                    line = f"{line[:max_job_line_chars - 3]}..."
                line = repr(line)
                print(f"[{queue_num}]", line)

def thread_run(queue: Queue, queue_num: int, sentinel, old_run_code, ip):
    """Loop over jobs in queue and execute them until sentinel is passed"""
    while True:
        obj = queue.get()
        try:
            if obj is sentinel:
                del jobs[queue_num]
                return
            
            try:
                code_obj, result, async_ = obj
                try:
                    with ip.builtin_trap, ip.display_trap:
                        old_run_code(
                            code_obj, result, async_=async_
                        ).send(None)
                except StopIteration:
                    pass
            finally:
                jobs[queue_num].pop(0)
        finally:
            queue.task_done()


async def new_run_code(code_obj, result=None, *, async_=False):
    """Send code objects to jobs thread, unless it is a bg magic"""
    if ext_state != "enabled" or id(sentinel) in code_obj.co_consts:
        return await old_run_code(code_obj, result, async_=async_)
    else:
        jobs[num_queues].append(code_obj)
        queues[-1].put((code_obj, result, async_))
        return False

def new_thread():
    """Create new jobs thread"""
    global num_queues
    num_queues += 1
    queues.append(Queue())
    jobs[num_queues] = []
    thread = Thread(
        target=thread_run,
        args=(queues[-1], num_queues, sentinel, old_run_code, ip)
    )
    thread.start()

def bgtransform(lines):
    """Identify bg magics and add sentinel to code"""
    if ext_state != "enabled":
        return lines

    return [
        re.sub(magic_call_pattern, magic_call_repl, line)
        for line in lines
    ]

def load_ipython_extension(ipython):
    """Load bg extension"""
    global ip, old_run_code, ext_state
    
    if ext_state == "never loaded":
        ip = ipython
        ip.input_transformers_post.append(bgtransform)
        for magic in bgmagics:
            register_line_magic(magic)

        old_run_code = ip.run_code
        ip.run_code = new_run_code

    ext_state = "enabled"
    new_thread()

def unload_ipython_extension(*_):
    global ext_state, queues, num_queues, jobs

    ext_state = "disabled"

    try:
        for queue in queues:
            queue.put(sentinel)

        while num_queues in jobs:
            time.sleep(timeout_step_secs)

        while jobs:
            time.sleep(timeout_step_secs)
    finally:
        queues = []
        num_queues = 0
        jobs = {}
