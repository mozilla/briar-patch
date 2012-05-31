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

import random
import socket


class Metric(object):
    """Helper class for Metrics.

    Translate counts and metrics and send to StatsD

    """
    def __init__(self, statsd=None):
        self.host   = None
        self.port   = None
        self.statsd = statsd

        if statsd is not None:
            if ':' in statsd:
                self.host, self.port = statsd.split(':')
                try:
                    self.port = int(self.port)
                except:
                    self.port = 2003
            else:
                self.host = statsd
                self.port = 8125

        self.address = (socket.gethostbyname(self.host), self.port)
        self.socket  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def incr(self, metric, count=1, rate=1):
        self._send(metric, '%s|c' % count, rate)

    def decr(self, metric, count=1, rate=1):
        self._send(metric, '%s|c' % -count, rate)

    def time(self, metric, seconds, rate=1):
        # time delta in milliseconds
        self._send(metric, '%d|ms' % (seconds / 1000), rate)

    def _send(self, metric, value, rate=1):
        if rate < 1:
            if random.random() < rate:
                value = '%s|@%s' % (value, rate)
            else:
                return
        try:
            self.socket.sendto('%s:%s' % (metric, value), self.address)
        except socket.error:
            pass
