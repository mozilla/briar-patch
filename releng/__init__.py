#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" releng

    :copyright: (c) 2011 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os, sys
import types
import json
import gzip
import urllib2
import logging
import StringIO
import subprocess

from optparse import OptionParser
from logging.handlers import RotatingFileHandler
from multiprocessing import get_logger

import redis

import version


_version_   = version.version
_copyright_ = version.copyright
_license_   = version.license


log      = get_logger()
_ourPath = os.getcwd()
_ourName = os.path.splitext(os.path.basename(sys.argv[0]))[0]
_secrets = {}


def relative(delta):
    if delta.days == 1:
        return '1 day ago'
    elif delta.days > 1:
        return '%d days ago' % delta.days
    elif delta.seconds <= 1:
        return 'now'
    elif delta.seconds < 60:
        return '%d seconds ago' % delta.seconds
    elif delta.seconds < 120:
        return '1 minute ago'
    elif delta.seconds < 3600:
        return '%d minutes ago' % (delta.seconds / 60)
    elif delta.seconds < 7200:
        return '1 hour ago'
    else:
        return '%d hours ago' % (delta.seconds / 3600)

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

    def exists(self, key):
        return self._redis.exists(key)

    def keys(self, search):
        return self._redis.keys(search)

    def expire(self, key, seconds=86400):
        return self._redis.expire(key, seconds)

    def lrange(self, listName, start, end):
        return self._redis.lrange(listName, start, end)

    def ltrim(self, listName, start, end):
        return self._redis.ltrim(listName, start, end)

    def lrem(self, listName, count, item):
        return self._redis.lrem(listName, count, item)

    def lpush(self, listName, item):
        return self._redis.lpush(listName, item)

    def rpush(self, listName, item):
        return self._redis.rpush(listName, item)

    def sadd(self, setName, item):
        return self._redis.sadd(setName, item)

    def srem(self, setName, item):
        return self._redis.srem(setName, item)

    def smembers(self, setName):
        return self._redis.smembers(setName)

    def sismember(self, setName, item):
        return self._redis.sismember(setName, item) == 1

    def set(self, key, value, expires=None):
        if expires is None:
            return self._redis.set(key, value)
        else:
            return self._redis.setex(key, expires, value)

    def incr(self, key):
        return self._redis.incr(key)

    def hincrby(self, key, field, increment=1):
        return self._redis.hincrby(key, field, increment)

    def hset(self, key, field, value):
        return self._redis.hset(key, field, value)

    def hget(self, key, field):
        return self._redis.hget(key, field)

    def hgetall(self, key):
        return self._redis.hgetall(key)

def loadConfig(filename):
    result = {}
    if os.path.isfile(filename):
        try:
            result = json.loads(' '.join(open(filename, 'r').readlines()))
        except:
            log.error('error during loading of config file [%s]' % filename, exc_info=True)
    return result

def initOptions(defaults=None, params=None):
    """Parse command line parameters and populate the options object.
    """
    parser = OptionParser()

    defaultOptions = { 'config':  ('-c', '--config',  '',            'Configuration file'),
                       'debug':   ('-d', '--debug',   False,         'Enable Debug'),
                       'logpath': ('-l', '--logpath', '',            'Path where log file is to be written'),
                       'verbose': ('-v', '--verbose', False,         'show extra output from remote commands'),
                       'dryrun':  ('',   '--dryrun',  False,         'do not perform any action if True'),
                       'force':   ('',   '--force',   False,         'force processing of a kitten even if it is in the seen cache'),
                       'tools':   ('',   '--tools',   '',            'path to tools checkout'),
                       'secrets': ('',   '--secrets', 'secrets.cfg', 'passwords - json dictionary with user/pw entries'),
                     }

    if params is not None:
        for key in params:
            defaultOptions[key] = params[key]

    if defaults is not None:
        for key in defaults:
            defaultOptions[key] = defaultOptions[key][0:2] + (defaults[key],) + defaultOptions[key][3:]

    for key in defaultOptions:
        items = defaultOptions[key]

        (shortCmd, longCmd, defaultValue, helpText) = items

        if type(defaultValue) is types.BooleanType:
            parser.add_option(shortCmd, longCmd, dest=key, action='store_true', default=defaultValue, help=helpText)
        else:
            parser.add_option(shortCmd, longCmd, dest=key, default=defaultValue, help=helpText)

    (options, args) = parser.parse_args()
    options.args    = args
    options.appPath = _ourPath

    if options.config is None:
        s = os.path.join(_ourPath, '%s.cfg' % _ourName)
        if os.path.isfile(s):
            options.config = s

    if options.config is not None:
        options.config = os.path.abspath(options.config)

        if not os.path.isfile(options.config):
            options.config = os.path.join(_ourPath, '%s.cfg' % options.config)

        if not os.path.isfile(options.config):
            options.config = os.path.abspath(os.path.join(_ourPath, '%s.cfg' % _ourName))

        jsonConfig = loadConfig(options.config)

        for key in jsonConfig:
            setattr(options, key, jsonConfig[key])

    if options.logpath is not None:
        options.logpath = os.path.abspath(options.logpath)

        if os.path.isdir(options.logpath):
            options.logfile = os.path.join(options.logpath, '%s.log'% _ourName)
        else:
            options.logfile = None

    if 'background' not in defaultOptions:
        options.background = False

    if options.tools is None:
        options.tools = '/builds/tools'

    return options

def initLogs(options, chatty=True, loglevel=logging.INFO):
    if options.logpath is not None:
        fileHandler   = RotatingFileHandler(os.path.join(options.logpath, '%s.log' % _ourName), maxBytes=1000000, backupCount=99)
        fileFormatter = logging.Formatter('%(asctime)s %(levelname)-7s %(processName)s: %(message)s')

        fileHandler.setFormatter(fileFormatter)

        log.addHandler(fileHandler)
        log.fileHandler = fileHandler

    if not options.background:
        echoHandler = logging.StreamHandler()

        if chatty:
            echoFormatter = logging.Formatter('%(levelname)-7s %(processName)s: %(message)s')
        else:
            echoFormatter = logging.Formatter('%(levelname)-7s %(message)s')

        echoHandler.setFormatter(echoFormatter)

        log.addHandler(echoHandler)
        log.info('echoing')

    if options.debug:
        log.setLevel(logging.DEBUG)
        log.info('debug level is on')
    else:
        log.setLevel(loglevel)

def getPassword(username):
    if username in _secrets:
        return _secrets[username]
    else:
        return None

def setPassword(username, password):
    _secrets[username] = password


def initKeystore(options):
    if options.secrets is not None and os.path.isfile(options.secrets):
        secrets = json.load(open(options.secrets, 'r'))
        for user in secrets:
            setPassword(user, secrets[user])

def runCommand(cmd, env=None, logEcho=True):
    """Execute the given command.
    Sends to the logger all stdout and stderr output.
    """
    log.debug('calling [%s]' % ' '.join(cmd))

    o = []
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    try:
        for item in p.stdout:
            o.append(item[:-1])
            if logEcho:
                log.debug(item[:-1])
        p.wait()
    except KeyboardInterrupt:
        p.kill()
        p.wait()

    return p, o

class DefaultErrorHandler(urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, fp, code, msg, headers):
        result = urllib2.HTTPError(req.get_full_url(), code, msg, headers, fp)
        result.status = code
        return result

def fetchUrl(url, debug=False):
    result = None
    opener = urllib2.build_opener(DefaultErrorHandler())
    opener.addheaders.append(('Accept-Encoding', 'gzip'))

    try:
        response = opener.open(url)
        raw_data = response.read()

        if response.headers.get('content-encoding', None) == 'gzip':
            result = gzip.GzipFile(fileobj=StringIO.StringIO(raw_data)).read()
        else:
            result = raw_data
    except:
        log.error('Error fetching url [%s]' % url, exc_info=True)

    return result

class Daemon(object):
    def __init__(self, pidfile):
        self.stdin   = '/dev/null'
        self.stdout  = '/dev/null'
        self.stderr  = '/dev/null'
        self.pidfile = pidfile

    def handlesigterm(self, signum, frame):
        if self.pidfile is not None:
            try:
                os.remove(self.pidfile)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                pass
        sys.exit(0)

    def start(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, exc:
            sys.stderr.write("%s: failed to fork from parent: (%d) %s\n" % (sys.argv[0], exc.errno, exc.strerror))
            sys.exit(1)

        os.chdir("/")
        os.setsid()
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                sys.stdout.close()
                sys.exit(0)
        except OSError, exc:
            sys.stderr.write("%s: failed to fork from parent #2: (%d) %s\n" % (sys.argv[0], exc.errno, exc.strerror))
            sys.exit(1)

        sys.stdout.flush()
        sys.stderr.flush()

        si = open(self.stdin, "r")
        so = open(self.stdout, "a+")
        se = open(self.stderr, "a+", 0)

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        if self.pidfile is not None:
            open(self.pidfile, "wb").write(str(os.getpid()))

        signal.signal(signal.SIGTERM, self.handlesigterm)

    def stop(self):
        if self.pidfile is None:
            sys.exit("no pidfile specified")
        try:
            pidfile = open(self.pidfile, "rb")
        except IOError, exc:
            sys.exit("can't open pidfile %s: %s" % (self.pidfile, str(exc)))
        data = pidfile.read()
        try:
            pid = int(data)
        except ValueError:
            sys.exit("mangled pidfile %s: %r" % (self.pidfile, data))
        os.kill(pid, signal.SIGTERM)

