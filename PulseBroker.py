#!/usr/bin/env python

""" PulseBroker

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

import os
import json
import time

from Queue import Empty
from multiprocessing import Process, Queue, current_process, get_logger, log_to_stderr

import zmq
import redis

from releng import initOptions, initLogs, dumpException

from mozillapulse import consumers


appInfo    = 'bear@mozilla.com|briar-patch'
log        = get_logger()
eventQueue = Queue()

MSG_TIMEOUT = 120  # 2 minutes until a pending message is considered "expired"

def cbMessage(data, message):
    """ cbMessage
    Parses the incoming pulse event and create a "job" that will be sent
    to a job processing server via ZeroMQ router.
    
    The job is placed into an event queue for async processing.
    """
    routingKey = data['_meta']['routing_key']
    msgType    = routingKey.split('.')[0]
    payload    = data['payload']

    job              = {}
    job['master']    = data['_meta']['master_name']
    job['pulse_key'] = routingKey
    job['time']      = data['_meta']['sent']
    job['id']        = data['_meta']['message_id']
    job['pulse']     = data

    if msgType == 'build':
        if 'build' in payload:
            job['slave'] = payload['build']['slave']
            job['event'] = 'build'

            pushJob(job)

    elif msgType == 'slave':
        if 'slave' in payload:
            job['slave'] = payload['slave']['name']
            job['event'] = 'slave connect'
        else:
            job['slave'] = payload['slavename']
            job['event'] = 'slave disconnect'

        pushJob(job)


def handleZMQ(options, events):
    """ handleZMQ
    Primary event loop for everything ZeroMQ related
    
    All payloads to be sent onward arrive via the event queue.
    
    Currently it is a very simple implementation of the Freelance
    pattern with no server heartbeat checks.
    
    The incoming events are structured as a list that always
    begins with the event type.
    
        Connect Server: ['connect', 'protocol://ip:port']
        Job:            ['job',     "{'payload': 'sample'}"]
        Heartbeat:      ['ping',]
    
    The structure of the message sent between nodes is:
    
        [destination, sequence, control, payload]
    
    all items are sent as strings.
    """
    log.info('starting')

    servers      = {}
    actives      = []
    request      = None
    expires      = None
    nextSequence = 0
    context      = zmq.Context()
    poller       = zmq.Poller()
    router       = context.socket(zmq.ROUTER)
    poller.register(router, zmq.POLLIN)

    events.put(('connect', 'tcp://localhost:5555'))

    while True:
        if request is not None:
            # current request has timed out, drop it
            if time.time() > expires:
                log.warning('request has expired %s' % ','.join(request))
                request = None
        else:
            try:
                event = events.get(False)
            except Empty:
                event = None

            if event is not None:
                eventType = event[0]
                log.debug('processing [%s]' % eventType)

                if eventType == 'connect':
                    endpoint = event[1]

                    if options.debug:
                        log.debug('connecting to server %s' % endpoint)

                    router.connect(endpoint)
                    servers[endpoint] = { 'endpoint':  endpoint,
                                          'alive':     False,
                                          'heartbeat': time.time(),
                                        }

                    time.sleep(0.1)
                    events.put(('ping', endpoint))

                elif eventType == 'ping':
                    endpoint = event[1]
                    request  = [endpoint, '0', 'ping']
                    expires  = time.time() + MSG_TIMEOUT

                    servers[endpoint]['alive']     = False
                    servers[endpoint]['heartbeat'] = time.time()

                    if options.debug:
                        log.debug('ping %s' % endpoint)

                    router.send_multipart(request)

                elif eventType == 'job':
                    if actives:
                        while actives:
                            endpoint = actives[0]
                            server   = servers[endpoint]

                            if server['alive']:
                                request = [endpoint, str(nextSequence), 'job', event[1]]
                                expires = time.time() + MSG_TIMEOUT

                                if options.debug:
                                    log.debug('send %s [%s]' % (endpoint, event[1]))

                                router.send_multipart(request)
                                break
                    else:
                        log.warning('no active servers, writing job to archive file')
                        # TODO - push archived item to redis

                else:
                    log.warning('unknown event [%s]' % eventType)

            n = time.time()
            for server in servers:
                if n - servers[server]['heartbeat'] > MSG_TIMEOUT:
                    events.put(('ping', server))

        try:
            items = dict(poller.poll(100))
        except:
            break

        if router in items:
            reply    = router.recv_multipart()
            endpoint = reply.pop(0)
            server   = servers[endpoint]

            server['heartbeat'] = time.time()

            if not server['alive']:
                log.debug('mark %s as alive' % endpoint)

                actives.append(endpoint)
                server['alive'] = True

            sequence = reply.pop(0)

            if options.debug:
                log.debug('recv %s [%s]' % (endpoint, reply[0]))

            if int(sequence) == nextSequence:
                nextSequence += 1
                request       = None

    log.info('done')


def pushJob(job):
    s = json.dumps(job)
    eventQueue.put(('job', s))


_defaultOptions = { 'config':      ('-c', '--config',       None,             'Configuration file'),
                    'debug':       ('-d', '--debug',        True,            'Enable Debug', 'b'),
                    'appinfo':     ('-a', '--appinfo',      appInfo,          'Mozilla Pulse app string'),
                    'background':  ('-b', '--background',   False,            'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',      None,             'Path where log file is to be written'),
                    'redis':       ('-r', '--redis',        'localhost:6379', 'Redis connection string'),
                    'pulsetopic':  ('-p', '--pulsetopic',   '#',              'Mozilla Pulse Topic filter string'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    Process(name='zmq', target=handleZMQ, args=(options, eventQueue,)).start()

    # log.info('Connecting to Mozilla Pulse with topic "%s"' % options.pulsetopic)
    pulse = consumers.BuildConsumer(applabel=options.appinfo)
    pulse.configure(topic=options.pulsetopic, callback=cbMessage)

    # log.debug('Starting pulse.listen()')
    pulse.listen()

