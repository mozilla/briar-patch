#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" Host reboot tool

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
        -w --workers        How many worker processes to spawn
        -k --kittens        What source to use for list of kittens
                            This can be a url, filename or a regex
                            default: http://build.mozilla.org/builds/slaves_needing_reboot.txt
           --dryrun         Do not perform any action, just list what would be done
           --filterbase
        -d --debug          Turn on debug logging
                            default: False
        -l --logpath        Path where the log file output is written
                            default: None
        -b --background     Fork to a daemon process
                            default: False

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os
import re
import json
import datetime
import smtplib
import email.utils

from email.mime.text import MIMEText
from multiprocessing import Process, Queue, get_logger
from Queue import Empty

from boto.ec2 import connect_to_region

from releng import initOptions, initLogs, fetchUrl, dbRedis, initKeystore, relative, getPassword
import releng.remote


log         = get_logger()
workQueue   = Queue()
resultQueue = Queue()
keyExpire   = 172800 # 2 days in seconds (1 day = 86,400 seconds)

urlNeedingReboot = 'http://build.mozilla.org/builds/slaves_needing_reboot.txt'


_defaultOptions = { 'kittens':    ('-k', '--kittens',    None,     'file or url to use as source of kittens'),
                    'filter':     ('-f', '--filter',     None,     'regex filter to apply to list'),
                    'environ':    ('',   '--environ',    'prod',   'which environ to process, defaults to prod'),
                    'workers':    ('-w', '--workers',    '1',      'how many workers to spawn'),
                    'filterbase': ('',   '--filterbase', '^%s',    'string to insert filter expression into'),
                    'cachefile':  ('',   '--cachefile',  None,     'filename to store the "have we touched this kitten before" cache'),
                    'force':      ('',   '--force',      False,    'force processing of a kitten. This ignores the seen cache *AND* SlaveAlloc', 'b'),
                    'email':      ('-e', '--email',      False,    'send result email', 'b'),
                    'monitor':    ('-m', '--monitor',    False,    'forces processing of any kitten that has not been processed at all', 'b'),
                    'redis':      ('-r', '--redis',     'localhost:6379', 'Redis connection string'),
                    'redisdb':    ('',   '--redisdb',   '8',              'Redis database'),
                    'smtpServer': ('',   '--smtpServer', None,     'where to send generated email to'),
                  }


def generate(hostlist, tag, indent=''):
    s = '\r\n%s\r\n' % tag
    t = ''
    m = []
    for item in hostlist:
        t += item
        m.append(item)

        if len(t) > 50:
            s += '%s%s\r\n' % (indent, ', '.join(m))
            t = ''
            m = []

    s += '%s    %s\r\n' % (', '.join(m), indent)

    return s

def previouslySeen(hostlist, lastrun):
    result = ''
    l      = []
    for kitten in lastrun:
        if kitten in hostlist:
            l.append(kitten)
            hostlist.remove(kitten)

    if len(l) > 0:
        result = generate(l, 'previously seen', '    ')

    return result

def getHistory(kitten):
    result = ''
    keys   = db.keys('kittenherder:*:%s' % kitten)
    keys.sort(reverse=True)
    for key in keys:
        d = db.hgetall(key)
        indent  = '    %s ' % key.replace('kittenherder:', '').replace(':%s' % kitten, '')
        result += indent

        for f in ('reachable', 'buildbot'):
            if f in d:
                result += '%s: %s ' % (f, d[f])
        result += '\r\n'
        result += ' ' * len(indent)
        for f in ('reboot', 'recovery', 'lastseen'):
            if f in d:
                result += '%s: %s ' % (f, d[f])
        result += '\r\n'

    return result

#        bm-xserve20 {'recovery': True, 'ipmi': False, 'output': ['adding to recovery list because host is not reachable', 'adding to recovery list because last activity is unknown'],
#                     'tacfile': '', 'pdu': False, 'fqdn': 'bm-xserve20.build.sjc1.mozilla.com.', 'reboot': False, 'reachable': False, 'lastseen': None,
#                     'buildbot': '', 'master': ''}

def sendEmail(data, smtpServer=None):
    if len(data) > 0:
        rebootedOS   = []
        rebootedIPMI = []
        rebootedPDU  = []
        recovered    = []
        idle         = []
        neither      = []
        body         = ''

        lastRun = db.lrange('kittenherder:lastrun', 0, -1)
        db.ltrim('kittenherder:lastrun', 0, 0)
        db.expire('kittenherder:lastrun', keyExpire)

        print lastRun
        for kitten, result in data:
            db.lpush('kittenherder:lastrun', kitten)

            print len(result), kitten, result
            if len(result) > 0:
                if result['reboot']:
                    if result['ipmi']:
                        rebootedIPMI.append(kitten)
                    elif result['pdu']:
                        rebootedPDU.append(kitten)
                    else:
                        rebootedOS.append(kitten)
                elif result['recovery']:
                    recovered.append(kitten)
                elif 'idle' in result['buildbot']:
                    idle.append(kitten)
                else:
                    if not result['reachable']:
                        neither.append(kitten)

        if len(idle) > 0:
            body += '\r\nbored kittens\r\n    %s\r\n' % ', '.join(idle)

        if len(rebootedOS) > 0:
            prevSeen = previouslySeen(rebootedOS, lastRun)
            body += generate(rebootedOS, 'rebooted (SSH)')
            body += prevSeen

        if len(rebootedPDU) > 0:
            prevSeen = previouslySeen(rebootedPDU, lastRun)
            body += generate(rebootedPDU, 'rebooted (PDU)')
            body += prevSeen

        if len(rebootedIPMI) > 0:
            prevSeen = previouslySeen(rebootedIPMI, lastRun)
            body += generate(rebootedIPMI, 'rebooted (IPMI)')
            body += prevSeen

        if len(recovered) > 0:
            body += '\r\nrecovery needed\r\n'
            for kitten in recovered:
                body += '%s\r\n%s' % (kitten, getHistory(kitten))

        if len(neither) > 0:
            body += '\r\nbear needs to look into these\r\n    %s\r\n' % ', '.join(neither)

        if len(body) > 0:
            addr = 'release@mozilla.com'
            msg  = MIMEText(body)

            msg.set_unixfrom('briarpatch')
            msg['To']      = email.utils.formataddr(('RelEng',     addr))
            msg['From']    = email.utils.formataddr(('briarpatch', addr))
            msg['Subject'] = '[briar-patch] idle kittens report'

            print body
            if smtpServer is not None:
                server = smtplib.SMTP(smtpServer)
                server.set_debuglevel(True)
                server.sendmail(addr, [addr], msg.as_string())
                server.quit()

def processKittens(options, jobs, results):
    remoteEnv = releng.remote.RemoteEnvironment(options.tools, db=db)
    dNow      = datetime.datetime.now()
    dDate     = dNow.strftime('%Y-%m-%d')
    dHour     = dNow.strftime('%H')

    while True:
        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            r = {}
            if job in remoteEnv.hosts:
                info = remoteEnv.hosts[job]
                if info['environment'] == options.environ:
                    if not info['enabled'] and not options.force:
                        if options.verbose:
                            log.info('%s not enabled, skipping' % job)
                    elif len(info['notes']) > 0 and not options.force:
                        if options.verbose:
                            log.info('%s has a slavealloc notes field, skipping' % job)
                    else:
                        log.info(job)
                        host = remoteEnv.getHost(job)
                        if host is None:
                            log.error('unknown host for %s' % job)
                        else:
                            r = remoteEnv.check(host, indent='    ', dryrun=options.dryrun, verbose=options.verbose)
                            d = remoteEnv.rebootIfNeeded(host, lastSeen=r['lastseen'], indent='    ', dryrun=options.dryrun, verbose=options.verbose)

                            for s in ['reboot', 'recovery', 'ipmi', 'pdu']:
                                r[s] = d[s]
                            r['output'] += d['output']

                            hostKey = 'kittenherder:%s.%s:%s' % (dDate, dHour, job)
                            for key in r:
                                db.hset(hostKey, key, r[key])
                            db.expire(hostKey, keyExpire)

                            # all this because json cannot dumps() the timedelta object
                            td = r['lastseen']
                            if td is not None:
                                secs             = td.seconds
                                hours, remainder = divmod(secs, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                r['lastseen']    = { 'hours':    hours,
                                                     'minutes':  minutes,
                                                     'seconds':  seconds,
                                                     'relative': relative(td),
                                                     'since':    secs,
                                                   }
                            log.info('%s: %s' % (job, json.dumps(r)))

                            if (host.farm == 'ec2') and (r['reboot'] or r['recovery']):
                                log.info('shutting down ec2 instance')
                                try:
                                    conn = connect_to_region(host.info['region'],
                                                             aws_access_key_id=getPassword('aws_access_key_id'),
                                                             aws_secret_access_key=getPassword('aws_secret_access_key'))
                                    conn.stop_instances(instance_ids=[host.info['id'],])
                                except:
                                    log.error('unable to stop ec2 instance %s [%s]' % (job, host.info['id']), exc_info=True)
                else:
                    if options.verbose:
                        log.info('%s not in requested environment %s (%s), skipping' % (job, options.environ, info['environment']))
            else:
                if options.verbose:
                    log.error('%s not listed in slavealloc, skipping' % job, exc_info=True)

            results.put((job, r))

def loadCache(cachefile):
    result = {}
    if os.path.isfile(cachefile):
        for item in open(cachefile, 'r+'):
            kitten, s = item.split(' ')
            ts        = datetime.datetime.strptime(s.strip(), '%Y-%m-%dT%H:%M:%S')
            now       = datetime.datetime.now()
            elapsed   = now - ts
            seconds   = (elapsed.days * 86400) + elapsed.seconds
            if seconds <= 3600:
                result[kitten] = ts

    return result

def writeCache(cachefile, cache):
    h = open(cachefile, 'w+')
    for kitten in cache.keys():
        ts = cache[kitten]
        h.write('%s %s\n' % (kitten, ts.strftime('%Y-%m-%dT%H:%M:%S')))
    h.close()

def loadKittenList(options):
    result = []

    if options.kittens.lower() in ('ec2',):
        for item in db.smembers('farm:%s:active' % options.kittens):
            result.append(db.hget(item, 'Name'))

    elif options.kittens.lower().startswith('http://'):
        # fetch url, and yes, we assume it's a text file
        items = fetchUrl(options.kittens)
        # and then make it iterable
        if items is not None:
            result = items.split('\n')

    elif os.path.exists(options.kittens):
        result = open(options.kittens, 'r').readlines()

    else:
        if ',' in options.kittens:
            result = options.kittens.split(',')
        else:
            result.append(options.kittens)

    return result


if __name__ == "__main__":
    options = initOptions(params=_defaultOptions)

    initLogs(options, chatty=False)

    if options.cachefile is None:
        options.cachefile = os.path.join(options.appPath, 'kittenherder_seen.dat')

    if options.kittens is None:
        options.kittens = urlNeedingReboot

    if options.filter is not None:
        reFilter = re.compile(options.filterbase % options.filter)
    else:
        reFilter = None

    db = dbRedis(options)

    log.info('Starting')

    initKeystore(options)

    # if reFilter is None:
    #     log.error("During this testing phase I'm making it so that --filter is required")
    #     log.error("Please re-run and specify a filter so we don't accidently process all")
    #     log.error("slaves or something silly like that -- thanks (bear)")
    #     sys.exit(1)

    if options.verbose:
        log.info('retrieving list of kittens to wrangle')

    seenCache = loadCache(options.cachefile)
    kittens   = loadKittenList(options)

    if len(kittens) > 0:
        results = []
        workers = []
        try:
            w = int(options.workers)
        except:
            log.error('invalid worker count value [%s] - using default of 4' % options.workers)
            w = 4
        for n in range(0, w):
            p = Process(target=processKittens, args=(options, workQueue, resultQueue))
            p.start()
            workers.append(p)

        # one slave per line:
        #    slavename, enabled yes/no
        #   talos-r4-snow-078,Yes
        #   tegra-050,No
        for item in kittens:
            try:
                if ',' in item:
                    kitten = item.split(',')[0]
                else:
                    kitten = item

                if reFilter is not None and reFilter.search(kitten) is None:
                    log.debug('%s rejected by filter' % kitten)
                    kitten = None
            except:
                kitten = None
                log.error('unable to parse line [%s]' % item, exc_info=True)

            if kitten is not None:
                if kitten in seenCache:
                    if options.force:
                        log.info("%s has been processed within the last hour but is being --force'd" % kitten)
                    else:
                        log.info('%s has been processed within the last hour, skipping' % kitten)
                        kitten = None
                if kitten is not None:
                    workQueue.put(kitten)
                    results.append(kitten)

        if options.verbose:
            log.info('waiting for workers to finish...')

        emailItems = []
        while len(results) > 0:
            try:
                item = resultQueue.get(False)
            except Empty:
                item = None

            if item is not None:
                kitten, result = item
                emailItems.append(item)
                if kitten in results:
                    results.remove(kitten)
                    seenCache[kitten] = datetime.datetime.now()

        if options.email:
            sendEmail(emailItems, options.smtpServer)

        if options.verbose:
            log.info('workers should be all done - closing up shop')

        if len(workers) > 0:
            # now lets wait till they are all done
            for p in workers:
                p.terminate()
                p.join()

    writeCache(options.cachefile, seenCache)

    log.info('Finished')

