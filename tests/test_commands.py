from unittest import TestCase
from multinoderedis import MultiNodeRedis
from redis._compat import (unichr, u, b, iteritems)

__author__ = 'kevinsoup'

"""
    A lot of these tests are adopted from here:
    https://github.com/andymccurdy/redis-py.

    TODO(ks) - 5/19/14 - After implementing all functions, figure out how to
    pull in tests directly from other repo, rather than ad hoc adding them.
"""
class TestMultiNodeRedis(TestCase):
    def setUp(self):
        master_servers = ['node1|127.0.0.1:6379', 'node2|127.0.0.1:6370']
        self.rc = MultiNodeRedis(master_servers)

    def tearDown(self):
        self.rc.flushall()

    def test_delete(self):
        assert self.rc.delete('a') == 0
        self.rc['a'] = 'foo'
        assert self.rc.delete('a') == 1

    def test_expire(self):
        assert not self.rc.expire('a', 10)
        self.rc['a'] = 'foo'
        assert self.rc.expire('a', 10)
        assert 0 < self.rc.ttl('a') <= 10
        assert self.rc.persist('a')
        assert self.rc.ttl('a') == -1

    def test_flushall(self):
        assert self.rc.get('a') is None
        self.rc.set('a', 'b')
        assert self.rc.get('a') == 'b'
        self.rc.flushall()
        assert self.rc.get('a') is None

    def test_get_and_set(self):
        # get and set can't be tested independently of each other
        assert self.rc.get('a') is None
        byte_string = b('value')
        integer = 5
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        assert self.rc.set('byte_string', byte_string)
        assert self.rc.set('integer', 5)
        assert self.rc.set('unicode_string', unicode_string)
        assert self.rc.get('byte_string') == byte_string
        assert self.rc.get('integer') == b(str(integer))
        # TODO(ks) - 5/19/2014 - Figure out why this doesn't work.
        #assert self.rc.get('unicode_string').decode('utf-8') == unicode_string

    def test_hget_and_hset(self):
        self.rc.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert self.rc.hget('a', '1') == b('1')
        assert self.rc.hget('a', '2') == b('2')
        assert self.rc.hget('a', '3') == b('3')

        # field was updated, redis returns 0
        assert self.rc.hset('a', '2', 5) == 0
        assert self.rc.hget('a', '2') == b('5')

        # field is new, redis returns 1
        assert self.rc.hset('a', '4', 4) == 1
        assert self.rc.hget('a', '4') == b('4')

        # key inside of hash that doesn't exist returns null value
        assert self.rc.hget('a', 'b') is None

    def test_hgetall(self):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        self.rc.hmset('a', h)
        assert self.rc.hgetall('a') == h

    def test_hincrby(self):
        assert self.rc.hincrby('a', '1') == 1
        assert self.rc.hincrby('a', '1', amount=2) == 3
        assert self.rc.hincrby('a', '1', amount=-2) == 1

    def test_hmset(self):
        h = {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        assert self.rc.hmset('a', h)
        assert self.rc.hgetall('a') == h

    def test_incr(self):
        assert self.rc.incr('a') == 1
        assert self.rc['a'] == b('1')
        assert self.rc.incr('a') == 2
        assert self.rc['a'] == b('2')
        assert self.rc.incr('a', amount=5) == 7
        assert self.rc['a'] == b('7')

    def test_mget(self):
        assert self.rc.mget(['a', 'b']) == [None, None]
        self.rc['a'] = '1'
        self.rc['b'] = '2'
        self.rc['c'] = '3'
        assert self.rc.mget('a', 'other', 'b', 'c') == [b('1'), None, b('2'), b('3')]

    def test_mset(self):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert self.rc.mset(d)
        for k, v in iteritems(d):
            assert self.rc[k] == v

    def test_zrange(self):
        self.rc.zadd('a', a1=1, a2=2, a3=3)
        assert self.rc.zrange('a', 0, 1) == [b('a1'), b('a2')]
        assert self.rc.zrange('a', 1, 2) == [b('a2'), b('a3')]

        # withscores
        assert self.rc.zrange('a', 0, 1, withscores=True) == \
            [(b('a1'), 1.0), (b('a2'), 2.0)]
        assert self.rc.zrange('a', 1, 2, withscores=True) == \
            [(b('a2'), 2.0), (b('a3'), 3.0)]

        # custom score function
        assert self.rc.zrange('a', 0, 1, withscores=True, score_cast_func=int) == \
            [(b('a1'), 1), (b('a2'), 2)]