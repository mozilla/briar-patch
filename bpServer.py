#!/usr/bin/env python

""" bpServer

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
        -r --redis          Redis server connection string
        -d --debug          Turn on debug logging
        -l --logpath        Path where the log file output is written
        -b --background     Fork to a daemon process

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
import redis

from releng import initOptions, initLogs, dumpException


log        = get_logger()
eventQueue = Queue()


def worker(events):
    log.info('starting')

    while True:
        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            print event

    log.info('done')


_defaultOptions = { 'config':     ('-c', '--config',     None,             'Configuration file'),
                    'debug':      ('-d', '--debug',      True,             'Enable Debug', 'b'),
                    'background': ('-b', '--background', False,            'daemonize ourselves', 'b'),
                    'logpath':    ('-l', '--logpath',    None,             'Path where log file is to be written'),
                    'redis':      ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    Process(name='worker', target=worker, args=(eventQueue,)).start()


    context         = zmq.Context()
    bindEndpoint    = 'tcp://*:5555'
    connectEndpoint = 'tcp://localhost:5555'

    server          = context.socket(zmq.ROUTER)
    server.identity = connectEndpoint
    server.bind(bindEndpoint)

    log.info('Server listening at %s' % bindEndpoint)

    while True:
        try:
            request = server.recv_multipart()
        except:
            dumpException('break during recv_multipart()')
            break

        # [ destination, sequence, control, payload ]
        address, sequence, control = request[:3]

        if control == 'ping':
            reply = [address, sequence, 'pong']
        else:
            eventQueue.put(request[3])

            reply = [address, sequence, 'ok']

        server.send_multipart(reply)

    log.info('done')

