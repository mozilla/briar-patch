#!/usr/bin/env python

""" releng - metrics

    where all the funky bits happen in the realm of
    incrementing things by 1 very fast

    :copyright: (c) 2011 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import json
import time
import socket
import logging

from multiprocessing import get_logger

from releng.constants import METRICS_COUNT, METRICS_TIME, METRICS_SET


log = get_logger()


class Metric(object):
    """Base class for Metrics.
    
    Sets up the required infrastructure to enable interval and count
    based metric tracking.
    
        Counters    basic increment/decrement, umm, counters ;)
                    all values sent to counters are rolled up into intervals
    
    """
    def __init__(self, graphite):
        self.carbon    = None
        self.counts    = {}
        self.intervals = (1, 5, 15)  # minutes
        self.last      = []
        for i in range(0, len(self.intervals)):
            self.last.append(0)

        log.info('Metrics configured for %d intervals' % len(self.intervals))

        if ':' in graphite:
            host, port = graphite.split(':')
            try:
                port = int(port)
            except:
                port = 2003
        else:
            host = graphite
            port = 2003

        try:
            self.carbon = socket.socket()
            self.carbon.connect((host, port))
        except:
            log.error('unable to connect to graphite at %s:%s' % (host, port), exc_info=True)
            self.carbon = None

    def check(self):
        now     = time.time()
        minutes = time.gmtime(now)[4]

        for i in range(0, len(self.intervals)):
            interval = self.intervals[i]
            p        = divmod(minutes, interval)[0]

            if p > self.last[i]:
                print '-'*42
                log.debug('gathering counts for interval %d' % interval)

                self.last[i] = p
                s            = ''
                for metric in self.counts:
                    m = self.counts[metric]
                    v = m['value'][i]
                    l = len(m['items'][i])
                    if l > 0:
                        avg = v / l
                        s  += '%s_%d %d %s\n'        % (metric, interval, v, now)
                        s  += '%s_%d_avg %0.3f %s\n' % (metric, interval, avg, now)

                        print i, metric, v, l, m['value']

                        m['value'][i] = 0
                        m['items'][i] = []

                        if i < len(self.intervals) - 1:
                            m['value'][i + 1] += v
                            m['items'][i + 1].append(v)
                            print i + 1, metric, m['value']

                if len(s) > 0:
                    log.debug('Sending to graphite [%s]' % s)
                    self.carbon.send(s)

    def count(self, metric, value=1):
        if metric in self.counts:
            self.counts[metric]['value'][0] += value
            self.counts[metric]['items'][0].append(value)
        else:
            v = []
            l = []
            for i in range(0, len(self.intervals)):
                v.append(0)
                l.append([])
            self.counts[metric] = { 'value': v,
                                    'items': l,
                                  }


def hashStore(db, hashKey, metric, items):
    db.hincrby(hashKey, metric)
    key = metric
    for item in items:
        key += ':%s' % item
        db.hincrby(hashKey, key)

