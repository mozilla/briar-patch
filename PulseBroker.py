#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" PulseBroker

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
                            default: None
        -r --redis          Redis server connection string
                            default: localhost:6379
           --redisdb        Redis database ID
                            default: 8
        -p --pulse          Pulse server connection string
                            default: None (i.e. Mozilla's Pulse)
        -t --topic          Pulse topic string
                            default: #
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

import json
import time

from Queue import Empty
from multiprocessing import Process, Queue, get_logger

import zmq

from releng import initOptions, initLogs, dbRedis
from releng.constants import ID_PULSE_WORKER

from mozillapulse import consumers


appInfo    = 'bear@mozilla.com|briar-patch-%s' + time.time()
log        = get_logger()
eventQueue = Queue()

SERVER_CHECK_INTERVAL = 30  # how often, in seconds, to check for new servers
PING_FAIL_MAX         = 1   # how many pings can fail before server is marked inactive
PING_INTERVAL         = 30  # ping servers every 2 minutes
MSG_TIMEOUT           = 30  # how long to wait in seconds until a pending message is considered expired


def OfflineTest(options):
    log.info('Starting Offline message testing')

    hArchive = open(options.testfile, 'r+')

    for msg in hArchive:
        job = json.loads(msg)
        pushJob(job)

    hArchive.close()


def cbMessage(data, message):
    """ cbMessage
    Parses the incoming pulse event and create a "job" that will be sent
    to a job processing server via ZeroMQ router.

    The job is placed into an event queue for async processing.
    """
    message.ack()

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

    elif msgType == 'change':
        job['event'] = 'source'
        pushJob(job)


class zmqService(object):
    def __init__(self, serverID, router, db, events):
        self.router  = router
        self.db      = db
        self.events  = events
        self.id      = serverID
        self.address = self.id.replace('%s:' % ID_PULSE_WORKER, '')
        if ':' not in self.address:
            self.address = '%s:5555' % self.address
        self.address = 'tcp://%s' % self.address

        self.init()

        log.debug('connecting to server %s' % self.address)

        self.router.connect(self.address)
        time.sleep(0.1)

    def init(self):
        self.payload  = None
        self.expires  = None
        self.sequence = 0
        self.errors   = 0
        self.alive    = True
        self.lastPing = time.time()

    def isAvailable(self):
        return self.alive and self.payload is None

    def reply(self, reply):
        if options.debug:
            log.debug('reply %s' % self.id)

        sequenceReply = reply.pop(0)

        if options.debug:
            log.debug('recv %s [%s]' % (self.id, reply[0]))

        self.errors   = 0
        self.alive    = True
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
                log.debug('send %s %d chars [%s]' % (self.id, len(msg), msg[:42]))

            self.router.send_multipart(self.payload)

            return True
        else:
            return False

    def heartbeat(self):
        if self.payload is not None and time.time() > self.expires:
            if self.payload[2] == 'ping':
                log.warning('server %s has failed to respond to %d ping requests' % (self.id, self.errors))
                if self.errors >= PING_FAIL_MAX:
                    self.alive = False
                else:
                    self.ping(force=True)
            else:
                log.warning('server %s has expired request: %s [%s]' % (self.id, ','.join(self.payload[:3]), self.payload[3][:42]))
                self.alive = False

        if self.alive:
            if time.time() - self.lastPing > PING_INTERVAL:
                self.ping()
        else:
            log.error('removing %s from active server list' % self.id)
            db.sadd('%s:inactive' % ID_PULSE_WORKER, self.id)

    def ping(self, force=False):
        if options.debug:
            log.debug('ping %s' % self.id)

        if force or self.isAvailable():
            self.sequence += 1
            self.payload   = [self.id, str(self.sequence), 'ping']
            self.lastPing  = time.time()
            self.expires   = self.lastPing + MSG_TIMEOUT
            self.errors   += 1
            self.alive     = False

            self.router.send_multipart(self.payload)
        else:
            log.warning('ping requested for offline service [%s]' % self.id)

def discoverServers(servers, db, events, router):
    for serverID in db.lrange(ID_PULSE_WORKER, 0, -1):
        if db.sismember('%s:inactive' % ID_PULSE_WORKER, serverID):
            log.warning('server %s found in inactive list, disconnecting' % serverID)
            if serverID in servers:
                servers[serverID].alive = False
        else:
            if serverID in servers:
                if servers[serverID].alive == False:
                    log.info('server %s found, resetting' % serverID)
                    servers[serverID].init()
            else:
                log.debug('server %s is new, adding to connect queue' % serverID)
                servers[serverID] = zmqService(serverID, router, db, events)

def handleZMQ(options, events, db):
    """ handleZMQ
    Primary event loop for everything ZeroMQ related

    All payloads to be sent onward arrive via the event queue.

    Currently it is a very simple implementation of the Freelance
    pattern with no server heartbeat checks.

    The incoming events are structured as a list that always
    begins with the event type.

        Job:            ('job',  "{'payload': 'sample'}")
        Heartbeat:      ('ping',)

    The structure of the message sent between nodes is:

        [destination, sequence, control, payload]

    all items are sent as strings.
    """
    log.info('starting')

    servers       = {}
    lastDiscovery = time.time()

    context = zmq.Context()
    router  = context.socket(zmq.ROUTER)
    poller  = zmq.Poller()
    poller.register(router, zmq.POLLIN)

    while True:
        available = False
        for serverID in servers:
            if servers[serverID].isAvailable():
                available = True
                break

        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            if available:
                eventType = event[0]

                if eventType == 'exit':
                    log.info('exit command received, terminating')
                    break

                if eventType == 'ping':
                    if event[1] in servers and servers[event[1]].alive:
                        servers[event[1]].ping()

                elif eventType == 'job':
                    handled = False
                    for serverID in servers:
                        if servers[serverID].isAvailable() and servers[serverID].request(event[1]):
                            handled = True
                            break
                    if not handled:
                        log.error('no active servers to handle request')
                        # TODO - push archived item to redis

                else:
                    log.warning('unknown event [%s]' % eventType)
            else:
                log.error('no active servers to handle request')

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
                if servers[serverID].alive:
                    servers[serverID].heartbeat()

        if time.time() > lastDiscovery:
            discoverServers(servers, db, events, router)
            lastDiscovery = time.time() + SERVER_CHECK_INTERVAL

    log.info('done')

def pushJob(job):
    s = json.dumps(job)
    eventQueue.put(('job', s))


_defaultOptions = { 'config':      ('-c', '--config',     None,             'Configuration file'),
                    'debug':       ('-d', '--debug',      True,             'Enable Debug'),
                    'appinfo':     ('-a', '--appinfo',    appInfo,          'Mozilla Pulse app string'),
                    'background':  ('-b', '--background', False,            'daemonize ourselves'),
                    'logpath':     ('-l', '--logpath',    None,             'Path where log file is to be written'),
                    'redis':       ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                    'redisdb':     ('',   '--redisdb',    '8',              'Redis database'),
                    'pulse':       ('-p', '--pulse',      None,             'Pulse connection string'),
                    'topic':       ('-t', '--topic',      '#',              'Mozilla Pulse Topic filter string'),
                    'testfile':    ('',   '--testfile',   None,             'Offline testing, uses named file instead of Pulse server'),
                  }


if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)

    log.info('Starting')

    log.info('Connecting to datastore')
    db = dbRedis(options)

    log.info('Creating ZeroMQ handler')
    Process(name='zmq', target=handleZMQ, args=(options, eventQueue, db)).start()

    if options.testfile:
        OfflineTest(options)
    else:
        try:
            log.info('Connecting to Mozilla Pulse with topic "%s"' % options.topic)
            pulse = consumers.BuildConsumer(applabel=options.appinfo)
            pulse.configure(topic=options.topic, callback=cbMessage)

            log.debug('Starting pulse.listen()')
            pulse.listen()
        except:
            log.error('Pulse Exception', exc_info=True)
            eventQueue.put(('exit',))
