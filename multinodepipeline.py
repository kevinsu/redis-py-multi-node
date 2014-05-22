class MultiNodePipeline(object):
    # TODO(ks) - 5/20/14 - Figure out what do with with shard_hint
    def __init__(self, multinodemanager, transaction=True, shard_hint=None):
        self.manager = multinodemanager
        self.counter = 0
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.pipeline_map = {}
        self.pipeline_order = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

    def __len__(self):
        return sum([len(ordering) for ordering in self.pipeline_order.values()])

    def _get_pipeline(self, key):
        node = self.manager._get_node(key)
        if node not in self.pipeline_map:
            pipeline = node.pipeline(transaction=self.transaction,
                                     shard_hint=self.shard_hint)
            self.pipeline_map[node] = pipeline
            self.pipeline_order[pipeline] = []
        return self.pipeline_map[node]

    def _update_pipeline(self, key):
        pipeline = self._get_pipeline(key)
        self.pipeline_order[pipeline].append(self.counter)
        self.counter += 1
        return pipeline

    def delete(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.delete(key)
        return self

    def execute(self):
        output = [None] * len(self)
        for pipeline in self.pipeline_map.values():
            order = self.pipeline_order[pipeline]
            for i, value in enumerate(pipeline.execute()):
                index = order[i]
                output[index] = value
        return output
        #for pipeline in self.pipeline_map.values():


    def expire(self, key, time):
        pipeline = self._update_pipeline(key)
        pipeline.expire(key, time)
        return self

    def get(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.get(key)
        return self

    def hget(self, key, field):
        pipeline = self._update_pipeline(key)
        pipeline.hget(key, field)
        return self

    def hgetall(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.hgetall(key)
        return self

    def hincrby(self, key, field, amount=1):
        pipeline = self._update_pipeline(key)
        pipeline.hincrby(key, field, amount=amount)
        return self

    def hmset(self, key, mapping):
        pipeline = self._update_pipeline(key)
        pipeline.hmset(key, mapping)
        return self

    def hset(self, key, field, value):
        pipeline = self._update_pipeline(key)
        pipeline.hset(key, field, value)
        return self

    def incr(self, key, amount=1):
        pipeline = self._update_pipeline(key)
        pipeline.incr(key, amount=amount)
        return self

    def llen(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.llen(key)
        return self

    def persist(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.persist(key)
        return self

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        pipeline = self._update_pipeline(key)
        pipeline.set(key, value)
        return self
        # TODO(ks) - 5/22/14 - Figure out why ex doesn't work
        #pipeline.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    def ttl(self, key):
        pipeline = self._update_pipeline(key)
        pipeline.ttl(key)
        return self

    def zadd(self, key, *args, **kwargs):
        pipeline = self._update_pipeline(key)
        pipeline.zadd(key, *args, **kwargs)
        return self

    def zincrby(self, key, value, amount=1):
        pipeline = self._update_pipeline(key)
        pipeline.zincrby(key, value, amount=amount)
        return self

    def zrange(self, key, start, end, desc=False, withscores=False,
               score_cast_func=float):
        pipeline = self._update_pipeline(key)
        pipeline.zrange(key, start, end, desc=desc,
                        withscores=withscores,
                        score_cast_func=score_cast_func)
        return self