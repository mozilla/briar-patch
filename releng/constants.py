#!/usr/bin/env python

""" releng - constants

    where all the funky bits happen in the realm of
    incrementing things by 1 very fast

    :copyright: (c) 2011 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

PORT_PULSE   = '5555'
PORT_METRICS = '5556'

ID_PULSE_WORKER   = 'pulse:workers'
ID_METRICS_WORKER = 'metrics:workers'

METRICS_COUNT = 'c'
METRICS_HASH  = 'h'
METRICS_KEY   = 'k'
METRICS_LIST  = 'l'
METRICS_SET   = 's'
