#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" bpServer

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
                            default: None
           --address        IP Address
                                127.0.0.1 or 127.0.0.1:5555
                            Required Value
        -r --redis          Redis server connection string
                            default: localhost:6379
           --redisdb        Redis database ID
                            default: 8
        -d --debug          Turn on debug logging
                            default: False
        -l --logpath        Path where the log file output is written
                            default: None
           --archivepath    Path where incoming jobs are to be archived
                            default: None
        -b --background     Fork to a daemon process
                            default: False

    Sample Configuration file

        { 'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os, sys
import time
import json

from datetime import date, datetime
from Queue import Empty
from multiprocessing import Process, Queue, get_logger

import zmq

from releng import initOptions, initLogs, dbRedis, getWorksteps
from releng.metrics import Metric
from releng.constants import PORT_PULSE, ID_PULSE_WORKER


log      = get_logger()
jobQueue = Queue()

ARCHIVE_CHUNK         = 100
SERVER_CHECK_INTERVAL = 30  # how long to wait in seconds before checking if we are being ignored


def getArchive(archivePath):
    if archivePath is not None and os.path.isdir(archivePath):
        d      = date.today()
        s      = os.path.join(archivePath, 'bp_archive_%s.dat' % d.strftime("%Y%m%d"))
        result = open(s, 'a+')
        log.info('archiving to %s' % s)
    else:
        result = None

    return result

def worker(jobs, db, archivePath, statsdServer):
    log.info('starting')

    aCount  = 0
    archive = getArchive(archivePath)

    pNames = ('branch', 'product', 'platform', 'revision', 'request_ids',
              'builduid', 'buildnumber', 'buildid', 'statusdb_id',
              'build_url', 'log_url', 'pgo_build', 'scheduler', 'who',
             )

    metric = Metric(statsdServer, archivePath)

    while True:
        try:
            entry = jobs.get(False)
        except Empty:
            entry = None

        if entry is not None:
            try:
                item = json.loads(entry)

                event    = item['event']
                key      = item['pulse_key']
                master   = item['master'].partition(':')[0].partition('.')[0]
                ts       = item['time']
                entryKey = key.split('.')[1]

                log.debug('Job: %s %s %s' % (event, key, ts))

                metric.incr('pulse', 1)

                if event == 'source':
                    properties = { 'revision':  None,
                                   'builduid':  None,
                                 }
                    try:
                        for p in item['pulse']['payload']['change']['properties']:
                            pName, pValue, _ = p
                            if pName in pNames:
                                properties[pName] = pValue
                    except:
                        log.error('exception extracting properties from build step', exc_info=True)

                    if properties['revision'] is None:
                        properties['revision'] = item['pulse']['payload']['change']['revision']

                    builduid  = properties['builduid']
                    changeKey = 'change:%s' % builduid

                    db.hset(changeKey, 'master',   master)
                    db.hset(changeKey, 'comments', item['pulse']['payload']['change']['comments'])
                    db.hset(changeKey, 'project',  item['pulse']['payload']['change']['project'])
                    db.hset(changeKey, 'branch',   item['pulse']['payload']['change']['branch'])

                    for p in properties:
                        db.hset(changeKey, p, properties[p])

                    tsDate, tsTime = ts.split('T')
                    tsHour         = tsTime[:2]

                    db.sadd('change:%s'    % tsDate,           changeKey)
                    db.sadd('change:%s.%s' % (tsDate, tsHour), changeKey)

                    metric.incr('change', 1)

                elif event == 'slave connect':
                    slave = item['slave']
                    metric.incr('machines.connect', 1)
                    metric.incr('machine.connect.%s' % slave, 1)

                elif event == 'slave disconnect':
                    slave = item['slave']
                    metric.incr('machines.disconnect', 1)
                    metric.incr('machine.disconnect.%s' % slave, 1)

                elif event == 'build':
                    items      = key.split('.')
                    buildEvent = items[-1]
                    project    = items[1]
                    slave      = item['slave']
                    properties = { 'branch':    None,
                                   'product':   None,
                                   'revision':  None,
                                   'builduid':  None,
                                 }
                    try:
                        for p in item['pulse']['payload']['build']['properties']:
                            pName, pValue, _ = p
                            if pName in pNames:
                                properties[pName] = pValue
                    except:
                        log.error('exception extracting properties from build step', exc_info=True)

                    product = properties['product']

                    if product in ('seamonkey',):
                        print 'skipping', product, event
                    else:
                        tStart    = item['time']
                        branch    = properties['branch']
                        builduid  = properties['builduid']
                        number    = properties['buildnumber']
                        buildKey  = 'build:%s'     % builduid
                        jobKey    = 'job:%s.%s.%s' % (builduid, master, number)
                        jobResult = item['pulse']['payload']['build']['results']

                        db.hset(jobKey, 'slave',   slave)
                        db.hset(jobKey, 'master',  master)
                        db.hset(jobKey, 'results', jobResult)

                        db.lpush('build:slave:jobs:%s' % slave, jobKey)
                        db.ltrim('build:slave:jobs:%s' % slave, 0, 20)

                        print jobKey, 'results', jobResult

                        for p in properties:
                            db.hset(jobKey, p, properties[p])

                        if 'scheduler' in properties:
                            scheduler = properties['scheduler']
                        else:
                            scheduler = 'None'
                        if 'platform' in properties:
                            platform = properties['platform']
                        else:
                            platform = 'None'

                            # jobs.:product.:platform.:scheduler.:master.:slave.:branch.:buildUID.results.:result
                        statskey = '%s.%s.%s.%s.%s.%s.%s' % (product, platform, scheduler, master, slave, branch, builduid)

                        if product == 'firefox':
                            metric.incr('jobs.results.%s.%s' % (statskey, jobResult), 1)

                        if buildEvent == 'started':
                            db.hset(jobKey, 'started', tStart)
                            if product == 'firefox':
                                metric.incr('jobs.start.%s' % statskey, 1)

                        elif buildEvent == 'finished':
                            if product == 'firefox':
                                metric.incr('jobs.end.%s' % statskey, 1)

                            # if started time is found, use that for the key
                            ts = db.hget(jobKey, 'started')
                            if ts is not None:
                                tStart = ts

                            dStarted   = datetime.strptime(tStart[:-6],       '%Y-%m-%dT%H:%M:%S')
                            dFinished  = datetime.strptime(item['time'][:-6], '%Y-%m-%dT%H:%M:%S')
                            tdElapsed  = dFinished - dStarted
                            secElapsed = (tdElapsed.days * 86400) + tdElapsed.seconds

                            db.hset(jobKey, 'finished', item['time'])
                            db.hset(jobKey, 'elapsed',  secElapsed)
                            if product == 'firefox':
                                #metric.time('build.%s' % statskey, secElapsed)
                                metric.incr('build.time.%s' % statskey, secElapsed)

                            builderName = item['pulse']['payload']['build']['builderName']
                            steps       = []
                            for step in item['pulse']['payload']['build']['steps']:
                                worksteps = getWorksteps(builderName)

                                if worksteps is not None:
                                    if step['name'] in worksteps:
                                        steps.append(step)
                                        continue
                            for step in steps:
                                stepStart = step['times'][0]
                                stepStop  = step['times'][1]
                                stepKey   = 'step:%s' % step['name']
                                db.hset(jobKey, '%s:start'   % stepKey, stepStart)
                                db.hset(jobKey, '%s:stop'    % stepKey, stepStop)
                                db.hset(jobKey, '%s:elapsed' % stepKey, stepStop - stepStart)

                        elif buildEvent == 'log_uploaded':
                            if 'request_ids' in properties:
                                db.hset(jobKey, 'request_ids', properties['request_ids'])

                        tsDate, tsTime = tStart.split('T')
                        tsHour         = tsTime[:2]

                        db.sadd('build:%s'    % tsDate,           buildKey)
                        db.sadd('build:%s.%s' % (tsDate, tsHour), buildKey)
                        db.sadd(buildKey, jobKey)

            except:
                log.error('Error converting incoming job', exc_info=True)

            if archive is not None:
                archive.write('%s\n' % entry)

            aCount += 1
            if aCount > ARCHIVE_CHUNK:
                if archive is not None:
                    archive.close()
                archive = getArchive(archivePath)
                aCount  = 0

    if archive is not None:
        archive.close()

    log.info('done')

def handleMessages(jobQueue, db, address, serverID):
    log.debug('binding to tcp://%s' % address)

    context = zmq.Context()
    server  = context.socket(zmq.ROUTER)

    server.identity = serverID
    server.bind('tcp://%s' % address)

    if db.sismember('%s:inactive' % ID_PULSE_WORKER, server.identity):
        log.info('removing ourselves from inactive list as we are just now starting')
        db.srem('%s:inactive' % ID_PULSE_WORKER, server.identity)

    log.info('Adding %s to the list of active servers' % server.identity)
    db.rpush(ID_PULSE_WORKER, server.identity)

    active = True
    while active:
        try:
            request = server.recv_multipart()

            # [ destination, sequence, control, payload ]
            address, sequence, control = request[:3]
            reply = [address, sequence]

            if control == 'ping':
                reply.append('pong')
            elif control == 'shutdown':
                log.info('being told to shutdown... exiting')
                active = False
            else:
                reply.append('ok')
                jobQueue.put(request[3])

            server.send_multipart(reply)
        except:
            log.error('error raised during recv_multipart()', exc_info=True)
            active = False

        if db.sismember('%s:inactive' % ID_PULSE_WORKER, server.identity):
            log.info('found our identity in the inactive list - exiting')
            active = False

    log.info('Removing ourselves to the list of active servers')
    db.lrem(ID_PULSE_WORKER, 0, server.identity)


_defaultOptions = { 'config':      ('-c', '--config',      '',               'Configuration file'),
                    'debug':       ('-d', '--debug',       True,             'Enable Debug'),
                    'logpath':     ('-l', '--logpath',     '',               'Path where log file is to be written'),
                    'address':     ('',   '--address' ,    '',               'IP Address'),
                    'archivepath': ('',   '--archivepath', '.',              'Path where incoming jobs are to be archived'),
                    'redis':       ('-r', '--redis',       'localhost:6379', 'Redis connection string'),
                    'redisdb':     ('',   '--redisdb',     '8',              'Redis database'),
                    'statsd':      ('',   '--statsd',      'localhost',      'StatsD server'),
                  }

if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)

    log.info('Starting')

    if options.address is None:
        log.error('Address is a required parameter, exiting')
        sys.exit(2)

    db = dbRedis(options)

    log.info('Creating processes')

    if ':' not in options.address:
        address = '%s:%s' % (options.address, PORT_PULSE)
    else:
        address = options.address
    serverID = '%s:%s' % (ID_PULSE_WORKER, address)

    pWorker   = Process(name='worker', target=worker, args=(jobQueue, db, options.archivepath, options.statsd))
    pMessages = Process(name='zeromq', target=handleMessages, args=(jobQueue, db, address, serverID))

    pWorker.start()
    pMessages.start()

    while True:
        time.sleep(SERVER_CHECK_INTERVAL)
        if db.sismember('%s:inactive' % ID_PULSE_WORKER, serverID):
            log.info('serverID found in inactive list - shutting down')
            break

    pMessages.terminate()
    pMessages.join()

    pWorker.terminate()
    pWorker.join()

    log.info('done')

