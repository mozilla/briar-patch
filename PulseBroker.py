#!/usr/bin/env python

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

SERVER_CHECK_INTERVAL = 120  # how often, in minutes, to check for new servers
PING_FAIL_MAX         = 1    # how many pings can fail before server is marked inactive
PING_INTERVAL         = 120  # ping servers every 2 minutes
MSG_TIMEOUT           = 120  # 2 minutes until a pending message is considered expired


class dbRedis(object):
    def __init__(self, options):
        if ':' in options.redis:
            host, port = options.redis.split(':')
            try:
                port = int(port)
            except:
                port = 6379
        else:
            host = options.redis
            port = 6379

        try:
            db = int(options.redisdb)
        except:
            db = 8

        log.info('dbRedis %s:%s db=%d' % (host, port, db))

        self.host   = host
        self.db     = db
        self.port   = port
        self._redis = redis.StrictRedis(host=host, port=port, db=db)

    def ping(self):
        return self._redis.ping()

    def lrange(self, listName, start, end):
        return self._redis.lrange(listName, start, end)

    def lrem(self, listName, count, item):
        return self._redis.lrem(listName, count, item)

    def rpush(self, listName, item):
        return self._redis.rpush(listName, item)

    def sadd(self, setName, item):
        return self._redis.sadd(setName, item)

    def sismember(self, setName, item):
        return self._redis.sismember(setName, item) == 1

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
    def __init__(self, serverID, router, db, events):
        self.id       = serverID
        self.router   = router
        self.db       = db
        self.events   = events
        self.payload  = None
        self.expires  = None
        self.sequence = 0
        self.errors   = 0
        self.alive    = True
        self.lastPing = time.time()

        self.router.connect(self.id)
        time.sleep(0.1)

    def isAvailable(self):
        return self.alive and self.payload is None

    def reply(self, reply):
        if options.debug:
            log.debug('reply %s' % self.id)

        sequenceReply = reply.pop(0)

        if options.debug:
            log.debug('recv %s [%s]' % (self.id, reply[0]))

        self.lastPing = time.time()
        self.errors   = 0
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
                    log.error('removing %s from server list' % self.id)
                    self.events.put(('disconnect', self.id))
                else:
                    self.ping(force=True)
            else:
                log.warning('server %s has expired request: %s' % (self.id, ','.join(self.payload)))
                self.ping()

        if time.time() - self.lastPing > PING_INTERVAL:
            self.ping()

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

def ping(serverID, servers):
    if serverID in servers:
        servers[serverID].ping()
    else:
        log.warning('ping request for unknown server %s' % serverID)

def discoverServers(servers, db, events):
    for serverID in db.lrange('pulse:workers', 0, -1):
        if db.sismember('pulse:workers:inactive', serverID):
            log.warning('server %s found in inactive list, disconnecting' % serverID)
            events.put(('disconnect', serverID))
        else:
            if serverID not in servers:
                log.debug('server %s is new, adding to connect queue' % serverID)
                events.put(('connect', serverID))

def removeServer(servers, serverID, db):
    db.sadd('pulse:workers:inactive', serverID)
    if serverID in servers:
        servers[serverID] = None
        del servers[serverID]

def handleZMQ(options, events, db):
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

    servers       = {}
    lastDiscovery = time.time()

    context = zmq.Context()
    router  = context.socket(zmq.ROUTER)
    poller  = zmq.Poller()
    poller.register(router, zmq.POLLIN)

    # events.put(('connect', 'tcp://localhost:5555'))

    while True:
        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            eventType = event[0]

            if eventType == 'connect':
                serverID          = event[1]
                servers[serverID] = zmqService(serverID, router, db, events)
                log.debug('connecting to server %s' % serverID)

            elif eventType == 'disconnect':
                removeServer(servers, event[1], db)

            elif eventType == 'ping':
                ping(servers, event[1])

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
                servers[serverID].heartbeat()

        if time.time() > lastDiscovery:
            discoverServers(servers, db, events)
            lastDiscovery = time.time() + SERVER_CHECK_INTERVAL

    log.info('done')

def pushJob(job):
    s = json.dumps(job)
    eventQueue.put(('job', s))


_defaultOptions = { 'config':      ('-c', '--config',     None,             'Configuration file'),
                    'debug':       ('-d', '--debug',      True,            'Enable Debug', 'b'),
                    'appinfo':     ('-a', '--appinfo',    appInfo,          'Mozilla Pulse app string'),
                    'background':  ('-b', '--background', False,            'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',    None,             'Path where log file is to be written'),
                    'redis':       ('-r', '--redis',      'localhost:6379', 'Redis connection string'),
                    'redisdb':     ('',   '--redisdb',    '8',              'Redis database'),
                    'pulse':       ('-p', '--pulse',      None,             'Pulse connection string'),
                    'topic':       ('-t', '--topic',     '#',               'Mozilla Pulse Topic filter string'),
                  }


if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    log.info('Connecting to datastore')
    db = dbRedis(options)

    log.info('Creating ZeroMQ handler')
    Process(name='zmq', target=handleZMQ, args=(options, eventQueue, db)).start()

    log.info('Connecting to Mozilla Pulse with topic "%s"' % options.topic)
    pulse = consumers.BuildConsumer(applabel=options.appinfo)
    pulse.configure(topic=options.topic, callback=cbMessage)

    log.debug('Starting pulse.listen()')
    pulse.listen()

