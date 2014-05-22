import zlib
import random
import redis
from multinodeexceptions import MultiNodeRedisException

NODE_NAME = "node_name"

class MultiNodeManager(object):
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
        self.node_map = {}
        for entry in master_servers:
            node_name, node = self._parse_config(entry)

            self.node_map[node_name] = [node]
        if slave_servers:
            for entry in slave_servers:
                node_name, node = self._parse_config(entry)
                if node_name not in self.node_map:
                    raise MultiNodeRedisException("slave with node_name: " +
                        "%s has no corresponding master." % node_name)
                self.node_map[node_name].append(node)

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

    ## Start private functions

    @classmethod
    def _parse_config(cls, entry):
        """
            Get node_name, redis client from a config entry.  Config entry is
            either of the form: node1|127.0.0.1:6379 or 127.0.0.1:6379.  In
            the second case, we grab the node name from the redis node.
        """
        if '|' in entry:
            node_name, server = entry.split('|')
        else:
            node_name = None
            server = entry
        host, port = server.split(":")
        node = redis.StrictRedis(host=host,
                                 port=int(port),
                                 db=0,
                                 decode_responses=True)
        node_name = node_name or node.get(NODE_NAME)
        if not node_name:
            raise MultiNodeRedisException("key: 'node_name' not set in " +
                "node %s:%s", host, port)
        return node_name, node

    def _get_node_name(self, key):
        """
            Deterministically get the node name associated with the specified
            key.
        """
        crc = zlib.crc32(key.encode('utf-8'))
        node_pos = crc % len(self.node_map)
        return self.node_map.keys()[node_pos]

    def _get_node(self, key, use_slave=False):
        """
            Get the node associated with the key.
        """
        node_name = self._get_node_name(key)
        nodes = self.node_map[node_name]
        if use_slave:
            if len(nodes) <= 1:
                raise MultiNodeRedisException("use_slave=True but no slaves " +
                    "have been initialized for node_name:%s." % node_name)
            return random.choice(nodes[1:])
        return nodes[0]

    def _get_all_nodes(self):
        return [nodes[0] for nodes in self.node_map.values()]