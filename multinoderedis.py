from multinodepipeline import MultiNodePipeline
from multinodemanager import MultiNodeManager
from multinodeexceptions import MultiNodeRedisException

class MultiNodeRedis(object):
    def __init__(self, master_servers, slave_servers=None):
        """
            Initialize MultiNodeRedis object.  The master hosts should contain
            the key "node_name".  This key identifies the node, and will be
            used to maintain a node mapping for sharding data.
            Args:
                master_servers - e.g. [node1|127.0.0.1:6379,
                                       node2|127.0.0.1:6370]
                slave_servers - same as master_hosts
        """
        self.manager = MultiNodeManager(master_servers, slave_servers=slave_servers)

    def __setitem__(self, name, value):
        self.set(name, value)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value:
            return value
        raise KeyError(name)

    @classmethod
    def _list_or_args(cls, keys, args):
        # returns a single list combining keys and args
        try:
            iter(keys)
            # a string or bytes instance can be iterated, but indicates
            # keys wasn't passed as a list
            if isinstance(keys, (basestring, bytes)):
                keys = [keys]
        except TypeError:
            keys = [keys]
        if args:
            keys.extend(args)
        return keys

    def _get_node(self, key, use_slave=False):
        return self.manager._get_node(key, use_slave=use_slave)

    def _get_all_nodes(self):
        return self.manager._get_all_nodes()

    def delete(self, key):
        node = self._get_node(key)
        return node.delete(key)

    def expire(self, key, time):
        node = self._get_node(key)
        return node.expire(key, time)

    def flushall(self):
        for node in self._get_all_nodes():
            node.flushall()

    def get(self, key):
        node = self._get_node(key)
        return node.get(key)

    def hget(self, key, field):
        node = self._get_node(key)
        return node.hget(key, field)

    def hgetall(self, key):
        node = self._get_node(key)
        return node.hgetall(key)

    def hincrby(self, key, field, amount=1):
        node = self._get_node(key)
        return node.hincrby(key, field, amount=amount)

    def hmset(self, key, mapping):
        node = self._get_node(key)
        return node.hmset(key, mapping)

    def hset(self, key, field, value):
        node = self._get_node(key)
        return node.hset(key, field, value)

    def incr(self, key, amount=1):
        node = self._get_node(key)
        return node.incr(key, amount=amount)

    def llen(self, key):
        node = self._get_node(key)
        return node.llen(key)

    def mget(self, keys, *args):
        args = self._list_or_args(keys, args)
        node_map = {}
        for key in args:
            node = self._get_node(key)
            if node not in node_map:
                node_map[node] = []
            node_map[node].append(key)
        mapping = {}
        for node, node_keys in node_map.iteritems():
            node_values = node.mget(node_keys)
            for i, node_key in enumerate(node_keys):
                mapping[node_key] = node_values[i]
        values = []
        for key in args:
            values.append(mapping.get(key, None))
        return values

    def mset(self, *args, **kwargs):
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise MultiNodeRedisException('MSET requires **kwargs or a ' +
                    'single dict arg')
            kwargs.update(args[0])
        node_map = {}
        for key, value in kwargs.iteritems():
            node = self._get_node(key)
            if node not in node_map:
                node_map[node] = {}
            node_map[node][key] = value
        success = 0
        for node, node_kwargs in node_map.iteritems():
            success = success or node.mset(node_kwargs)
        return success

    def persist(self, key):
        node = self._get_node(key)
        return node.persist(key)

    def pipeline(self, transaction=True, shard_hint=None):
        # TODO(ks) - 5/22/14 - I'm not actually sure if pipeline should return
        # the same pipeline object or a new one each time.
        return MultiNodePipeline(self.manager,
                                 transaction=transaction,
                                 shard_hint=shard_hint)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        node = self._get_node(key)
        return node.set(key, value)
        # TODO(ks) - 5/22/14 - Figure out why ex doesn't work
        # return node.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def ttl(self, key):
        node = self._get_node(key)
        return node.ttl(key)

    def zadd(self, key, *args, **kwargs):
        node = self._get_node(key)
        return node.zadd(key, *args, **kwargs)

    def zincrby(self, key, value, amount=1):
        node = self._get_node(key)
        return node.zincrby(key, value, amount=amount)

    def zrange(self, key, start, end, desc=False, withscores=False,
               score_cast_func=float):
        node = self._get_node(key)
        return node.zrange(key, start, end, desc=desc, withscores=withscores,
                           score_cast_func=score_cast_func)