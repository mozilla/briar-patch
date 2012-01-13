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
        -b --background     Fork to a daemon process
                            default: False

    Sample Configuration file

        { 'redis': 'localhost:6379',
          'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import sys
import logging

from Queue import Empty
from multiprocessing import Process, Queue, current_process, get_logger, log_to_stderr

import zmq

from releng import initOptions, initLogs, dbRedis
from releng.metrics import processJob

log        = get_logger()
eventQueue = Queue()


def worker(events, db):
    log.info('starting')

    while True:
        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            processJob(db, event)

    log.info('done')


_defaultOptions = { 'config':     ('-c', '--config',     None,             'Configuration file'),
                    'debug':      ('-d', '--debug',      True,             'Enable Debug', 'b'),
                    'background': ('-b', '--background', False,            'daemonize ourselves', 'b'),
                    'logpath':    ('-l', '--logpath',    None,             'Path where log file is to be written'),
                    'redis':      ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                    'redisdb':    ('',   '--redisdb',    '8',              'Redis database'),
                    'address':    ('',   '--address' ,   None,             'IP Address'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    if options.address is None:
        log.error('Address is a required parameter, exiting')
        sys.exit(2)

    log.info('Connecting to datastore')
    db = dbRedis(options)

    if db.ping():
        log.info('Creating job worker')
        Process(name='worker', target=worker, args=(eventQueue, db)).start()

        if ':' not in options.address:
            options.address = '%s:5555' % options.address

        log.debug('binding to tcp://%s' % options.address)

        context = zmq.Context()
        server  = context.socket(zmq.ROUTER)

        server.identity = 'pulse:worker:%s' % options.address
        server.bind('tcp://%s' % options.address)

        log.info('Adding %s to the list of active servers' % server.identity)
        db.rpush('pulse:workers', server.identity)

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
                eventQueue.put(request[3])

            server.send_multipart(reply)

        log.info('Removing ourselves to the list of active servers')
        db.lrem('pulse:workers', 0, server.identity)
    else:
        log.error('Unable to reach the database')

    log.info('done')

