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
import logging

from multiprocessing import get_logger

from releng.constants import METRICS_COUNT, METRICS_TIME, METRICS_SET


log = get_logger()


class Metric(object):
    """Base class for Metrics.
    
    Sets up the required infrastructure to enable interval and count
    based metric tracking.
    
        Counters    basic increment/decrement, umm, counters ;)
        Meters      rate over time, by default tracks 5, 15, 60 min averages
    
    """
    def __init__(self):
        self.tracking = {}

    def register(self, metricName, metricType=METRICS_COUNT):
        pass

def hashStore(db, hashKey, metric, items):
    db.hincrby(hashKey, metric)
    key = metric
    for item in items:
        key += ':%s' % item
        db.hincrby(hashKey, key)

# _qhr = ''
# _connects = 0
# _starts   = 0
# 
# 
# 
# ts     = job['time']
# 
# tsDate, tsTime = ts.split('T')
# tsHour = tsTime[:2]
# try:
#     tsQHour = divmod(int(tsTime[3:5]), 15)[0]
# except:
#     tsQHour = 0
# 
# 
# if tsQHour != _qhr:
#     carbon.put('%s %d %d' % ('bp:metric.connects', _connects, now))
#     carbon.put('%s %d %d' % ('bp:metric.starts',   _starts,   now))
#     _connects = 0
#     _starts   = 0
# _qhr = tsQHour
