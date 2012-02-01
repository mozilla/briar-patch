#!/usr/bin/env python

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
import socket
import logging

from Queue import Empty
from multiprocessing import Process, Queue, current_process, get_logger, log_to_stderr

import zmq

from releng import initOptions, initLogs, dbRedis
from releng.constants import PORT_PULSE, ID_PULSE_WORKER, ID_METRICS_WORKER, \
                             METRICS_COUNT, METRICS_TIME, METRICS_SET

log         = get_logger()
jobQueue    = Queue()
metricQueue = Queue()


def metric(jobs, options):
    log.info('starting')

    db = dbRedis(options)

    context = zmq.Context()
    router  = context.socket(zmq.ROUTER)
    poller  = zmq.Poller()
    poller.register(router, zmq.POLLIN)

    remoteID = None
    sequence = 0

    while True:
        if remoteID is None:
            for serverID in db.lrange(ID_METRICS_WORKER, 0, -1):
                if not db.sismember('%s:inactive' % ID_METRICS_WORKER, serverID):
                    remoteID = serverID
                    address  = remoteID.replace('%s:' % ID_METRICS_WORKER, '')

                    if ':' not in address:
                        address = '%s:%s' % (address, PORT_PULSE)
                    address = 'tcp://%s' % address

                    log.debug('connecting to server %s' % address)

                    router.connect(address)
                    time.sleep(0.1)

        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            msg = json.dumps(job)
            sequence += 1
            payload   = [remoteID, str(sequence), 'job', msg]

            if options.debug:
                log.debug('send %s %d chars [%s]' % (remoteID, len(msg), msg[:42]))

            router.send_multipart(payload)

        try:
            items = dict(poller.poll(100))
        except:
            break

        if router in items:
            reply = router.recv_multipart()

    log.info('done')


def worker(jobs, metrics, archivePath):
    log.info('starting')

    if archivePath is not None and os.path.isdir(archivePath):
        s       = os.path.join(archivePath, 'bp_archive.dat')
        archive = open(s, 'a+')
        log.info('archiving to %s' % s)
    else:
        archive = None

    while True:
        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            try:
                item = json.loads(job)

                event  = item['event']
                key    = item['pulse_key']
                slave  = item['slave']
                master = item['master'].partition(':')[0].partition('.')[0]
                ts     = item['time']

                log.debug('Job: %s %s' % (event, key))

                outbound = []

                if event == 'slave connect':
                    outbound.append((METRICS_COUNT, ('connect:slave',  slave )))
                    outbound.append((METRICS_COUNT, ('connect:master', master)))

                elif event == 'slave disconnect':
                    outbound.append((METRICS_COUNT, ('disconnect:slave',  slave )))
                    outbound.append((METRICS_COUNT, ('disconnect:master', master)))

                elif event == 'build':
                    items      = key.split('.')
                    buildEvent = items[-1]
                    project    = items[1]
                    properties = { 'branch':    None,
                                   'product':   None,
                                   'revision':  None,
                                   'builduid':  None,
                                 }
                    try:
                        for p in item['pulse']['payload']['build']['properties']:
                            pName, pValue, _ = p
                            if pName in ('branch', 'product', 'revision', 'builduid'):
                                properties[pName] = pValue
                    except:
                        log.error('exception extracting properties from build step', exc_info=True)

                    branch   = properties['branch']
                    product  = properties['product']
                    builduid = properties['builduid']

                    outbound.append((METRICS_SET, ('build:%s' % builduid, buildEvent, ts    )))
                    outbound.append((METRICS_SET, ('build:%s' % builduid, 'slave',    slave )))
                    outbound.append((METRICS_SET, ('build:%s' % builduid, 'master',   master)))

                    for p in properties:
                        outbound.append((METRICS_SET, ('build:%s' % builduid, p, properties[p])))

                    outbound.append((METRICS_COUNT, ('build', buildEvent)))

                    if buildEvent == 'started':
                        outbound.append((METRICS_COUNT, ('build:started:slave',   slave  )))
                        outbound.append((METRICS_COUNT, ('build:started:master',  master )))
                        outbound.append((METRICS_COUNT, ('build:started:branch',  branch )))
                        outbound.append((METRICS_COUNT, ('build:started:product', product)))

                    elif buildEvent == 'finished':
                        outbound.append((METRICS_COUNT, ('build:finished:slave',   slave  )))
                        outbound.append((METRICS_COUNT, ('build:finished:master',  master )))
                        outbound.append((METRICS_COUNT, ('build:finished:branch',  branch )))
                        outbound.append((METRICS_COUNT, ('build:finished:product', product)))

                metrics.put(outbound)

            except:
                log.error('Error converting incoming job to json', exc_info=True)

            if archive is not None:
                archive.write(event)
                archive.write('\n')

    if archive is not None:
        archive.close()

    log.info('done')


_defaultOptions = { 'config':      ('-c', '--config',      None,  'Configuration file'),
                    'debug':       ('-d', '--debug',       True,  'Enable Debug', 'b'),
                    'background':  ('-b', '--background',  False, 'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',     None,  'Path where log file is to be written'),
                    'address':     ('',   '--address' ,    None,  'IP Address'),
                    'archivepath': ('',   '--archivepath', '.',   'Path where incoming jobs are to be archived'),
                    'redis':       ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                    'redisdb':     ('',   '--redisdb',    '8',              'Redis database'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    if options.address is None:
        log.error('Address is a required parameter, exiting')
        sys.exit(2)

    db = dbRedis(options)

    log.info('Creating processes')
    Process(name='worker', target=worker, args=(jobQueue, metricQueue, options.archivepath)).start()
    Process(name='metric', target=metric, args=(metricQueue, options)).start()

    if ':' not in options.address:
        options.address = '%s:%s' % (options.address, PORT_PULSE)

    log.debug('binding to tcp://%s' % options.address)

    context = zmq.Context()
    server  = context.socket(zmq.ROUTER)

    server.identity = '%s:%s' % (ID_PULSE_WORKER, options.address)
    server.bind('tcp://%s' % options.address)

    log.info('Adding %s to the list of active servers' % server.identity)
    db.rpush(ID_PULSE_WORKER, server.identity)

    while True:
        try:
            request = server.recv_multipart()
        except:
            log.error('error raised during recv_multipart()', exc_info=True)
            break

        # [ destination, sequence, control, payload ]
        address, sequence, control = request[:3]
        reply = [address, sequence]

        if control == 'ping':
            reply.append('pong')
        else:
            reply.append('ok')
            jobQueue.put(request[3])

        server.send_multipart(reply)

    log.info('Removing ourselves to the list of active servers')
    db.lrem(ID_PULSE_WORKER, 0, server.identity)

    log.info('done')

