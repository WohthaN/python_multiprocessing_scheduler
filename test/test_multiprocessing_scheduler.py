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

import random
import time
import unittest
from unittest.mock import Mock, patch

from multiprocessing_scheduler import schedule_workers, _proc_f, FakeProcess


class CrashException(Exception):
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.args == other.args
        else:
            return False

    def __hash__(self):
        return id(self)


def return_args(*args, **kwargs):
    time.sleep(random.random() * 0.01)
    return args


def return_all(*args, **kwargs):
    return args, kwargs


def crash(*args, **kwargs):
    raise CrashException("ouch!")


class Test_proc_f(unittest.TestCase):
    def setUp(self):
        self.res_q = Mock()

    def test_f_is_called_correctly(self):
        _proc_f(return_all, self.res_q, 1, (), {})
        expected = {'pid': 1, 'exception': None, 'result': ((), {})}
        self.res_q.put.assert_called_once_with(expected)

    def test_retvals_are_correct(self):
        expected = {'pid': 10, 'exception': None, 'result': (tuple(range(10)), {'bla': 1})}
        _proc_f(return_all, self.res_q, 10, tuple(range(10)), {'bla': 1})
        self.res_q.put.assert_called_once_with(expected)

    def test_exception_is_handled(self):
        expected = {'pid': 10, 'exception': [CrashException("ouch!"), 'stacktrace'], 'result': None}
        _proc_f(crash, self.res_q, 10, (), {})

        self.assertEqual(len(self.res_q.put.call_args_list), 1)

        called_args = self.res_q.put.call_args_list[0][0][0]

        self.assertEqual(set(called_args.keys()), set(expected.keys()))
        self.assertEqual(called_args['pid'], expected['pid'])
        self.assertEqual(called_args['result'], expected['result'])
        raised_e, stacktrace = called_args['exception']
        self.assertEqual(raised_e, expected['exception'][0])
        self.assertTrue('test_multiprocessing_scheduler.py' in stacktrace)
        self.assertTrue('in crash' in stacktrace)
        self.assertTrue('raise CrashException("ouch!")' in stacktrace)


class Test_schedule_workers(unittest.TestCase):
    def test_all_jobs_are_scheduled(self):
        results = schedule_workers(return_args, list(zip(range(10))))
        self.assertEqual(len(results), 10)

    def test_one_arg(self):
        results = schedule_workers(return_args, list(zip(range(100))))
        self.assertEqual(sorted(results, key=lambda x: x[0]), list(zip(range(100))))

    def test_multiple_args(self):
        args = list(zip(range(100), range(100)))
        results = schedule_workers(return_args, args)
        self.assertEqual(sorted(results, key=lambda x: x[0]), args)

    def test_kwargs(self):
        args = list(zip(range(10)))
        kwargs = [{'thekwarg': i} for i in args]
        results = schedule_workers(return_all, list(zip(args, kwargs)), with_kwargs=True)

        self.assertEqual(len(results), 10)

        for r in results:
            self.assertEqual(r[0], r[1]['thekwarg'])

    def test_exceptions_are_forwarded(self):
        with self.assertRaises(CrashException):
            results = schedule_workers(crash, list(zip(range(10))))

    def test_exceptions_are_masked(self):
        schedule_workers(crash, zip(range(10)), forward_exceptions=False)

    def test_process_limit_is_respected(self):
        # Limiting to 1 process must ensure results are ordered
        results = schedule_workers(return_args, list(zip(range(100))), max_processes=1)
        self.assertEqual(results, list(zip(range(100))))

    def test_is_really_multiprocessing(self):
        results = schedule_workers(return_args, list(zip(range(200))), max_processes=100)
        self.assertEqual(sorted(results, key=lambda x: x[0]), list(zip(range(200))))
        # If it's really multiprocessing, with random waits we _must_ get a result which is out of order.
        with self.assertRaises(AssertionError):
            self.assertEqual(results, list(zip(range(200))))


class Test_schedule_workers_no_multiprocessing(unittest.TestCase):
    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_all_jobs_are_scheduled(self):
        results = schedule_workers(return_args, list(zip(range(10))))
        self.assertEqual(len(results), 10)

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_one_arg(self):
        results = schedule_workers(return_args, list(zip(range(100))))
        self.assertEqual(sorted(results, key=lambda x: x[0]), list(zip(range(100))))

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_multiple_args(self):
        args = list(zip(range(100), range(100)))
        results = schedule_workers(return_args, args)
        self.assertEqual(sorted(results, key=lambda x: x[0]), args)

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_kwargs(self):
        args = list(zip(range(10)))
        kwargs = [{'thekwarg': i} for i in args]
        results = schedule_workers(return_all, list(zip(args, kwargs)), with_kwargs=True)

        self.assertEqual(len(results), 10)

        for r in results:
            self.assertEqual(r[0], r[1]['thekwarg'])

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_exceptions_are_forwarded(self):
        with self.assertRaises(CrashException):
            results = schedule_workers(crash, list(zip(range(10))))

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_exceptions_are_masked(self):
        schedule_workers(crash, zip(range(10)), forward_exceptions=False)

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_process_limit_is_respected(self):
        # Limiting to 1 process must ensure results are ordered
        results = schedule_workers(return_args, list(zip(range(100))), max_processes=1)
        self.assertEqual(results, list(zip(range(100))))

    @patch("multiprocessing_scheduler.Process", FakeProcess)
    def test_is_not_multiprocessing(self):
        results = schedule_workers(return_args, list(zip(range(200))), max_processes=100)
        self.assertEqual(sorted(results, key=lambda x: x[0]), list(zip(range(200))))
        # If it's not multiprocessing, we should get results in order no matter what
        self.assertEqual(results, list(zip(range(200))))

    if __name__ == "__main__":
        unittest.main()
