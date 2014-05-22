from __future__ import with_statement
import pytest
from unittest import TestCase
from multinoderedis import MultiNodeRedis
import redis
from redis._compat import b, u, unichr, unicode


class TestPipeline(TestCase):
    def setUp(self):
        master_servers = ['node1|127.0.0.1:6379', 'node2|127.0.0.1:6370']
        self.rc = MultiNodeRedis(master_servers)

    def tearDown(self):
        self.rc.flushall()

    def test_pipeline(self):
        with self.rc.pipeline() as pipe:
            pipe.set('a', 'a1').get('a').zadd('z', z1=1).zadd('z', z2=4)
            pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
            assert pipe.execute() == \
                [
                    True,
                    b('a1'),
                    True,
                    True,
                    2.0,
                    [(b('z1'), 2.0), (b('z2'), 4)],
                ]

    def test_pipeline_length(self):
        with self.rc.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0
            assert not pipe

            # Fill 'er up!
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert len(pipe) == 3
            assert pipe

            # Execute calls reset(), so empty once again.
            pipe.execute()
            assert len(pipe) == 0
            assert not pipe

    def test_pipeline_no_transaction(self):
        with self.rc.pipeline(transaction=False) as pipe:
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert pipe.execute() == [True, True, True]
            assert self.rc['a'] == b('a1')
            assert self.rc['b'] == b('b1')
            assert self.rc['c'] == b('c1')

    def test_pipeline_no_transaction_watch(self):
        self.rc['a'] = 0

        with self.rc.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            pipe.multi()
            pipe.set('a', int(a) + 1)
            assert pipe.execute() == [True]

    def test_pipeline_no_transaction_watch_failure(self):
        self.rc['a'] = 0

        with self.rc.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            self.rc['a'] = 'bad'

            pipe.multi()
            pipe.set('a', int(a) + 1)

            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert self.rc['a'] == b('bad')

    def test_exec_error_in_response(self):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        self.rc['c'] = 'a'
        with self.rc.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            result = pipe.execute(raise_on_error=False)

            assert result[0]
            assert self.rc['a'] == b('1')
            assert result[1]
            assert self.rc['b'] == b('2')

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert self.rc['c'] == b('a')

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert self.rc['d'] == b('4')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert self.rc['z'] == b('zzz')

    def test_exec_error_raised(self):
        self.rc['c'] = 'a'
        with self.rc.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert unicode(ex.value).startswith('Command # 3 (LPUSH c 3) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert self.rc['z'] == b('zzz')

    def test_parse_error_raised(self):
        with self.rc.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert unicode(ex.value).startswith('Command # 2 (ZREM b) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert self.rc['z'] == b('zzz')

    def test_watch_succeed(self):
        self.rc['a'] = 1
        self.rc['b'] = 2

        with self.rc.pipeline() as pipe:
            pipe.watch('a', 'b')
            assert pipe.watching
            a_value = pipe.get('a')
            b_value = pipe.get('b')
            assert a_value == b('1')
            assert b_value == b('2')
            pipe.multi()

            pipe.set('c', 3)
            assert pipe.execute() == [True]
            assert not pipe.watching

    def test_watch_failure(self):
        self.rc['a'] = 1
        self.rc['b'] = 2

        with self.rc.pipeline() as pipe:
            pipe.watch('a', 'b')
            self.rc['b'] = 3
            pipe.multi()
            pipe.get('a')
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    def test_unwatch(self):
        self.rc['a'] = 1
        self.rc['b'] = 2

        with self.rc.pipeline() as pipe:
            pipe.watch('a', 'b')
            self.rc['b'] = 3
            pipe.unwatch()
            assert not pipe.watching
            pipe.get('a')
            assert pipe.execute() == [b('1')]

    def test_transaction_callable(self):
        self.rc['a'] = 1
        self.rc['b'] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get('a')
            assert a_value in (b('1'), b('2'))
            b_value = pipe.get('b')
            assert b_value == b('2')

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                self.rc.incr('a')
                has_run.append('it has')

            pipe.multi()
            pipe.set('c', int(a_value) + int(b_value))

        result = self.rc.transaction(my_transaction, 'a', 'b')
        assert result == [True]
        assert self.rc['c'] == b('4')

    def test_exec_error_in_no_transaction_pipeline(self):
        self.rc['a'] = 1
        with self.rc.pipeline(transaction=False) as pipe:
            pipe.llen('a')
            pipe.expire('a', 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert unicode(ex.value).startswith('Command # 1 (LLEN a) of '
                                                'pipeline caused error: ')

        assert self.rc['a'] == b('1')

    def test_exec_error_in_no_transaction_pipeline_unicode_command(self):
        key = unichr(3456) + u('abcd') + unichr(3421)
        self.rc[key] = 1
        with self.rc.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            print ex.value
            expected = unicode('Command # 1 (LLEN %s) of pipeline caused '
                               'error: ') % key
            assert unicode(ex.value).startswith(expected)

        assert self.rc[key] == b('1')