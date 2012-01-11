#!/usr/bin/env python

""" PulseBroker

    :copyright: (c) 2011 by Mozilla
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

import time
import json

from Queue import Empty
from multiprocessing import Process, Queue, current_process, get_logger, log_to_stderr

import zmq
import redis

from releng import initOptions, initLogs, dumpException

from mozillapulse import consumers


appInfo    = 'bear@mozilla.com|briar-patch'
log        = get_logger()
eventQueue = Queue()


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
    # job['payload']   = payload

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


def handleZMQ(events):
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
    
        [destination, sequence, event, payload]
    
    all items are sent as strings.
    """
    # log.info('handleZMQ start')

    servers      = {}
    actives      = []
    request      = None
    nextSequence = 0
    context      = zmq.Context()
    poller       = zmq.Poller()
    router       = context.socket(zmq.ROUTER)
    poller.register(router, zmq.POLLIN)

    while True:
        if request is not None:
            # if time.time() >= expires:
            #     request = None
            pass
        else:
            try:
                event = events.get(False)
            except Empty:
                event = None

            if event is not None:
                eventType = event[0]
                if eventType == 'connect':
                    endpoint = event[1]

                    if options.debug:
                        log.debug('connecting to server %s' % endpoint)

                    router.connect(endpoint)
                    servers[endpoint] = { 'endpoint': endpoint,
                                          'alive':    True,
                                        }
                    actives.append(endpoint)
                    time.sleep(0.1)

                elif eventType == 'job':
                    while actives:
                        endpoint = actives[0]
                        server   = servers[endpoint]

                        if server['alive']:
                            request = [server['endpoint'], str(nextSequence), event[1]]

                            if options.debug:
                                log.debug('send: %s [%s]' % (endpoint, event[1]))

                            router.send_multipart(request)
                            break

        try:
            items = dict(poller.poll(100))
        except:
            break

        if router in items:
            reply    = router.recv_multipart()
            endpoint = reply.pop(0)
            server   = servers[endpoint]

            if not server['alive']:
                actives.append(endpoint)
                server['alive'] = True

            sequence = reply.pop(0)

            if options.debug:
                log.debug('recv: %s [%s]' % (endpoint, reply[0]))

            if int(sequence) == nextSequence:
                nextSequence += 1
                request       = None


def pushJob(job):
    s = json.dumps(job)
    eventQueue.put(('job', s))


_defaultOptions = { 'config':     ('-c', '--config',     None,             'Configuration file'),
                    'debug':      ('-d', '--debug',      True,             'Enable Debug', 'b'),
                    'background': ('-b', '--background', False,            'daemonize ourselves', 'b'),
                    'logpath':    ('-l', '--logpath',    None,             'Path where log file is to be written'),
                    'redis':      ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                    'appinfo':    ('-a', '--appinfo',    appInfo,          'Mozilla Pulse app string'),
                    'pulsetopic': ('-p', '--pulsetopic', '#',              'Mozilla Pulse Topic filter string'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    Process(name='zmq', target=handleZMQ, args=(eventQueue,)).start()

    eventQueue.put(('connect', 'tcp://localhost:5555'))

    # log.info('Connecting to Mozilla Pulse with topic "%s"' % options.pulsetopic)
    pulse = consumers.BuildConsumer(applabel=options.appinfo)
    pulse.configure(topic=options.pulsetopic, callback=cbMessage)

    # log.debug('Starting pulse.listen()')
    pulse.listen()

