# -*- coding: utf-8 -*-

import json
import time
import datetime
import traceback
import collections

def _get_key(*keys, namespace='resque'):
    return ':'.join([namespace] + list(keys))

def _error_payload(queue, body, error):
    return {
        'worker': body['class'],
        'queue': queue,
        'payload': body,
        'exception': error.__class__.__name__,
        'error': error.message,
        'backtrace': traceback.formt_exc(),
        'failed_at': str(datetime.datetime.utcnow()),
    }

class MissingJobError(Exception):
    pass

class Resque(object):

    def __init__(self, conn, interval=5):
        self.conn = conn
        self.interval = interval
        self._registry = collections.defaultdict(dict)

    def work(self):
        while True:
            for queue in self._registry:
                key = _get_key('queue', queue)
                raw = self.conn.lpop(key)
                if raw:
                    self.perform(queue, raw)
            time.sleep(self.interval)

    def perform(self, queue, raw):
        body = json.loads(raw)
        try:
            task = self._registry[queue][body['class']]
        except KeyError:
            error = MissingJobError('No job named "{0}"'.format(body['class']))
            self._error(queue, body, error)
            return
        try:
            task(*body['args'])
            self._success(queue, body)
        except Exception as error:
            self._error(queue, body, error)

    def enqueue(self, queue, klass, *args):
        key = _get_key('queue', queue)
        value = {
            'class': klass,
            'args': args,
        }
        self.conn.rpush(key, json.dumps(value))

    def task(self, queue, klass=None):
        def wrapper(func):
            self._registry[queue][klass or func.__name__] = func
            return func
        return wrapper

    def _success(self, queue, body):
        self.conn.incr(_get_key('stat', 'processed'))
        self.conn.incr(_get_key('stat', 'processed', body['class']))

    def _error(self, queue, body, error):
        self.conn.incr(_get_key('stat', 'failed'))
        self.conn.incr(_get_key('stat', 'failed', body['class']))
        self.conn.rpush(
            _get_key('failed'),
            json.dumps(_error_payload(queue, body, error)),
        )
