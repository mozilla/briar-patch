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
import logging

from multiprocessing import get_logger


log = get_logger()

def processJob(db, msg):
    """processJob()
    
    Do the up-front parsing required to know what key, field and
    increment to use and then make the call
        key                     field           type/increment
        bp:metric:slavename     slavename       
    """
    try:
        job = json.loads(msg)

        event  = job['event']
        key    = job['pulse_key']
        slave  = job['slave']
        master = job['master'].split(':')[0]
        ts     = job['time']

        tsDate, tsTime = ts.split('T')
        tsHour = tsTime[:2]
        try:
            tsQHour = divmod(int(tsTime[3:4]), 15)[0]
        except:
            tsQHour = 0

        # try and sniff as fast as possible what Metric "Type" we have
        if event == 'slave connect':
            db.hincrby('bp:metric', 'connect:%s'       % slave,                    1)
            db.hincrby('bp:metric', 'connect:%s:%s'    % (slave, tsDate),          1)
            db.hincrby('bp:metric', 'connect:%s:%s:%s' % (slave, tsDate, tsHour),  1)
            db.hincrby('bp:metric', 'connect:%s'       % (tsDate),                 1)
            db.hincrby('bp:metric', 'connect:%s:%s'    % (tsDate, tsHour),         1)
            db.hincrby('bp:metric', 'connect:%s'       % master,                   1)
            db.hincrby('bp:metric', 'connect:%s:%s'    % (master, tsDate),         1)
            db.hincrby('bp:metric', 'connect:%s:%s:%s' % (master, tsDate, tsHour), 1)

        elif event == 'slave disconnect':
            db.hincrby('bp:metric', 'disconnect:%s'       % slave,                    1)
            db.hincrby('bp:metric', 'disconnect:%s:%s'    % (slave, tsDate),          1)
            db.hincrby('bp:metric', 'disconnect:%s:%s:%s' % (slave, tsDate, tsHour),  1)
            db.hincrby('bp:metric', 'disconnect:%s'       % (tsDate),                 1)
            db.hincrby('bp:metric', 'disconnect:%s:%s'    % (tsDate, tsHour),         1)
            db.hincrby('bp:metric', 'disconnect:%s'       % master,                   1)
            db.hincrby('bp:metric', 'disconnect:%s:%s'    % (master, tsDate),         1)
            db.hincrby('bp:metric', 'disconnect:%s:%s:%s' % (master, tsDate, tsHour), 1)

        elif event == 'build':
            l       = key.split('.')
            t       = l[-1]
            project = key.replace('build.', '').replace('.%s' % t, '')

            if t == 'finished':
                db.hincrby('bp:metric', 'build:%s'       % slave,                     1)
                db.hincrby('bp:metric', 'build:%s:%s'    % (slave, tsDate),           1)
                db.hincrby('bp:metric', 'build:%s:%s:%s' % (slave, tsDate, tsHour),   1)
                db.hincrby('bp:metric', 'build:%s'       % (tsDate),                  1)
                db.hincrby('bp:metric', 'build:%s:%s'    % (tsDate, tsHour),          1)
                db.hincrby('bp:metric', 'build:%s'       % master,                    1)
                db.hincrby('bp:metric', 'build:%s:%s'    % (master, tsDate),          1)
                db.hincrby('bp:metric', 'build:%s:%s:%s' % (master, tsDate, tsHour),  1)

                db.hincrby('bp:metric', 'build:%s'       % project,                   1)
                db.hincrby('bp:metric', 'build:%s:%s'    % (project, tsDate),         1)
                db.hincrby('bp:metric', 'build:%s:%s:%s' % (project, tsDate, tsHour), 1)

    except:
        log.error('Error converting incoming job to json', exc_info=True)