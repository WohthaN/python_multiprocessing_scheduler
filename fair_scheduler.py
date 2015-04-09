# Carli Samuele <carlisamuele@csspace.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import multiprocessing as mp
from multiprocessing import Queue
import os


def _proc_f(function, results_q, pid, args, kwargs):
    retdict = {'pid': pid, 'exception': None, 'result': None}

    try:
        retdict['result'] = function(*args, **kwargs)
    except Exception as e:
        retdict['exception'] = e
    finally:
        results_q.put(retdict)


def schedule_workers(
        function,
        args_list,
        with_kwargs=False,
        max_processes=mp.cpu_count(),
        forward_exceptions=True):
    """
    Somewhat similar to the map() builtin function, apply function to every item of args_list and return a list of the
    results. Each function application will fork a process which lives only for the time required to compute the
    function: for fast functions, this is probably a waste of resources and you will be better off using map() or
    multiprocessing.map() with a big chunksize.
    Differently from multiprocessing.map(), schedule_workers spawns a new process as soon as a previous one is finished,
    one by one. This will keep all your processors busy even if function takes a vastly different time to execute on
    different inputs.
    You can use kwargs by setting the with_kwargs flag and populating args_list as
    ((args_list1, kwargs1),(args_list2, kwargs2), ...) instead of (args_list1, args_list2, ...).

    :param function: any picklable f(*args, **kwargs).
    :param args_list: an iterable (args_list1, args_list2, ...) to each item of which function is applied.
    :param with_kwargs: if True, args_list should be an iterable with ((args_list1, kwargs1),(args_list2, kwargs2), ...)
    :param max_processes: spawn at most max_processes parallel processes.
    :param forward_exceptions: if False, exceptions raised by function are ignored. If True (the default),
           as soon as a process rises an exception, all other running processes are terminated and the exception
           is rised.
    :return: a list consisting of tuples containing items from all iterables. 
    """
    processes = dict()
    results_q = Queue()
    results = list()

    def new_process(pid, args, kwargs):
        new_proc = mp.Process(target=_proc_f, args=(function, results_q, pid, args, kwargs))
        new_proc.start()
        processes[pid] = new_proc

    def collect_result():
        result = results_q.get()
        if not result['exception']:
            results.append(result['result'])
        else:
            if forward_exceptions:
                for pid, proc in processes.items():
                    proc.terminate()
                raise result['exception']

        processes[result['pid']].join()
        processes[result['pid']].terminate()
        del processes[result['pid']]

    for pid, job in enumerate(args_list):
        if with_kwargs:
            args, kwargs = job
        else:
            args = job
            kwargs = dict()

        if len(processes) >= max_processes:
            collect_result()

        new_process(pid, args, kwargs)

    while len(processes) > 0:
        collect_result()

    return results