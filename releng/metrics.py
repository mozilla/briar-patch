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


log = get_logger()

def store(db, carbon, hashKey, metric, items):
    now = int(time.time())

    db.hincrby(hashKey, metric)

    rKey = metric
    cKey = '%s.%s' % (hashKey, metric)
    cKey = cKey.replace(':', '.')
    s    = '%s 1 %d\n' % (cKey, now)

    for item in items:
        rKey += ':%s' % item
        cKey += '.%s' % item
        db.hincrby(hashKey, rKey)
        s += '%s 1 %d\n' % (cKey, now)

    carbon.put(s)

def processJob(db, carbon, msg):
    """processJob()
    
    Do the up-front parsing required to know what key, field and
    increment to use and then make the call
    """
    try:
        job = json.loads(msg)

        event  = job['event']
        key    = job['pulse_key']
        slave  = job['slave']
        master = job['master'].partition(':')[0].partition('.')[0]
        ts     = job['time']

        tsDate, tsTime = ts.split('T')
        tsHour = tsTime[:2]
        try:
            tsQHour = divmod(int(tsTime[3:4]), 15)[0]
        except:
            tsQHour = 0

        log.debug('Job: %s %s' % (event, key))

        if event == 'slave connect':
            store(db, carbon, 'bp:metric:connect', tsDate, (tsHour, tsQHour))
            store(db, carbon, 'bp:metric:connect', slave,  (tsDate, tsHour, tsQHour))
            store(db, carbon, 'bp:metric:connect', master, (tsDate, tsHour, tsQHour))

        elif event == 'slave disconnect':
            store(db, carbon, 'bp:metric:disconnect', tsDate, (tsHour, tsQHour))
            store(db, carbon, 'bp:metric:disconnect', slave,  (tsDate, tsHour, tsQHour))
            store(db, carbon, 'bp:metric:disconnect', master, (tsDate, tsHour, tsQHour))

        elif event == 'build':
            items = key.split('.')
            buildEvent = items[-1]
            project    = items[1]
            properties = { 'branch':    None,
                           'product':   None,
                           'revision':  None,
                           'builduid':  None,
                         }
            try:
                for p in job['pulse']['payload']['build']['properties']:
                    pName, pValue, _ = p
                    if pName in ('branch', 'product', 'revision', 'builduid'):
                        properties[pName] = pValue
            except:
                log.error('exception extracting properties from build step', exc_info=True)

            branch   = properties['branch']
            product  = properties['product']
            builduid = properties['builduid']

            db.hset('build:%s' % builduid, buildEvent, ts)
            db.hset('build:%s' % builduid, 'slave',    slave)
            db.hset('build:%s' % builduid, 'master',   master)
            for p in properties:
                db.hset('build:%s' % builduid, p, properties[p])

            db.hincrby('bp:metric:build', buildEvent)

            if buildEvent == 'started':
                db.sadd('bp:metric:build:started', builduid)

            elif buildEvent == 'finished':
                if db.sismember('bp:metric:build:started', builduid):
                    db.srem('bp:metric:build:started', builduid)
                else:
                    log.warning('build %s %s has finished but start event not found' % (key, builduid))

                db.rpush('bp:metric:build:finished:%s' % tsDate, builduid)

                store(db, carbon, 'bp:metric:build', 'slave:%s'   % slave,   (tsDate, tsHour, tsQHour))
                store(db, carbon, 'bp:metric:build', 'master:%s'  % master,  (tsDate, tsHour, tsQHour))
                store(db, carbon, 'bp:metric:build', 'branch:%s'  % branch,  (tsDate, tsHour, tsQHour))
                store(db, carbon, 'bp:metric:build', 'product:%s' % product, (tsDate, tsHour, tsQHour))

    except:
        log.error('Error converting incoming job to json', exc_info=True)

