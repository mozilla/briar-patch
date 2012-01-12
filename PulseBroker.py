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


class zmqService(object):
    def __init__(self, serverID, router):
        self.id       = serverID
        self.router   = router
        self.payload  = None
        self.expires  = None
        self.sequence = 0
        self.errors   = 0
        self.alive    = True
        self.lastPing = time.time() # - MSG_TIMEOUT

        self.router.connect(self.id)
        time.sleep(0.1)

    def isAvailable(self):
        return self.alive and self.payload is None

    def expired(self):
        result = False
        # current request has timed out, drop it
        if self.payload is not None and time.time() > self.expires:
            log.warning('server %s has expired request: %s' % (self.id, ','.join(self.payload)))
            if self.payload[-1] == 'ping':
                self.errors += 1
                log.warning('server %s has failed to respond to %d ping requests' % (self.id, self.errors))

                if self.errors > 5:
                    result = True
            else:
                self.ping()

        return result

    def reply(self, reply):
        if options.debug:
            log.debug('reply %s' % self.id)

        sequenceReply = reply.pop(0)

        if options.debug:
            log.debug('recv %s [%s]' % (self.id, reply[0]))

        self.lastPing = time.time()
        self.expires  = self.lastPing + MSG_TIMEOUT

        if int(sequenceReply) == self.sequence:
            self.payload = None
        else:
            log.error('reply received out of sequence')

    def request(self, msg):
        if options.debug:
            log.debug('request %s' % self.id)

        if self.isAvailable():
            self.sequence += 1
            self.payload   = [self.id, str(self.sequence), 'job', msg]
            self.expires   = time.time() + MSG_TIMEOUT

            if options.debug:
                log.debug('send %s %d [%s]' % (self.id, len(msg), msg[:42]))

            self.router.send_multipart(self.payload)

            return True
        else:
            return False

    def heartbeat(self):
        if time.time() - self.lastPing > MSG_TIMEOUT:
            self.ping()

    def ping(self):
        if options.debug:
            log.debug('ping %s' % self.id)

        if self.isAvailable():
            self.sequence += 1
            self.payload  = [self.id, str(self.sequence), 'ping']
            self.lastPing = time.time()
            self.expires  = self.lastPing + MSG_TIMEOUT
            self.alive    = False

            self.router.send_multipart(self.payload)
        else:
            log.warning('ping requested for offline service [%s]' % self.id)

def ping(serverID, servers):
    if serverID in servers:
        servers[serverID].ping()
    else:
        log.warning('ping request for unknown server %s' % serverID)

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

    servers = {}
    context = zmq.Context()
    router  = context.socket(zmq.ROUTER)
    poller  = zmq.Poller()
    poller.register(router, zmq.POLLIN)

    events.put(('connect', 'tcp://localhost:5555'))

    while True:
        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            eventType = event[0]

            if eventType == 'connect':
                serverID          = event[1]
                servers[serverID] = zmqService(serverID, router)
                log.debug('connecting to server %s' % serverID)

            elif eventType == 'ping':
                ping(event[1], servers)

            elif eventType == 'job':
                handled = False
                for serverID in servers:
                    if servers[serverID].isAvailable() and servers[serverID].request(event[1]):
                        handled = True
                        break
                if not handled:
                    log.warning('no active servers to handle request')
                    events.put(('job', event[1]))
                    # TODO - push archived item to redis

            else:
                log.warning('unknown event [%s]' % eventType)

        try:
            items = dict(poller.poll(100))
        except:
            break

        if router in items:
            reply    = router.recv_multipart()
            serverID = reply.pop(0)
            servers[serverID].reply(reply)
        else:
            for serverID in servers:
                if servers[serverID].expired():
                    log.warning('server %s is being removed from list' % serverID)
                    del servers[serverID]
                    events.put(('connect', serverID))
                else:
                    servers[serverID].heartbeat()

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

