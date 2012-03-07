#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

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


log = get_logger()


class Metric(object):
    """Base class for Metrics.
    
    Sets up the required infrastructure to enable interval and count
    based metric tracking.
    
        Counters    basic increment/decrement, umm, counters ;)
                    all values sent to counters are rolled up into intervals
    
    """
    def __init__(self, graphite, db):
        self.host      = None
        self.port      = None
        self.db        = db
        self.counts    = {}
        self.intervals = (1, 5, 15)  # minutes
        self.last      = []
        for i in range(0, len(self.intervals)):
            self.last.append(0)

        log.info('Metrics configured for %d intervals' % len(self.intervals))

        if ':' in graphite:
            self.host, self.port = graphite.split(':')
            try:
                self.port = int(self.port)
            except:
                self.port = 2003
        else:
            self.host = graphite
            self.port = 2003

    def carbon(self, stats):
        try:
            sock = socket.socket()
            sock.connect((self.host, self.port))

            sock.send(stats)

            sock.close()
        except:
            log.error('unable to connect to graphite at %s:%s' % (self.host, self.port), exc_info=True)

    def check(self):
        now     = time.time()
        minutes = time.gmtime(now)[4]

        for i in range(0, len(self.intervals)):
            interval = self.intervals[i]
            p        = divmod(minutes, interval)[0]

            if p > self.last[i]:
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

                        hash = 'metrics'
                        if ':' in metric:
                            hash += ':%s' % metric.split(':', 1)[0]
                        self.db.hset(hash, '%s_%d' % (metric, interval), v)

                        m['value'][i] = 0
                        m['items'][i] = []

                        if i < len(self.intervals) - 1:
                            m['value'][i + 1] += v
                            m['items'][i + 1].append(v)

                if len(s) > 0:
                    log.debug('Sending to graphite [%s]' % s)
                    self.carbon(s)
            else:
                # handle case where we loop back to beginning of interval
                if p < self.last[i]:
                    self.last[i] = p

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

