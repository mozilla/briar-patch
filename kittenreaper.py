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

import sys, os
import re
import time
import json
import random
import logging
import datetime
import paramiko
import smtplib
import email.utils

from email.mime.text import MIMEText
from multiprocessing import Process, Queue, get_logger, log_to_stderr
from Queue import Empty

from releng import initOptions, initLogs, fetchUrl, runCommand
import releng.remote


log         = get_logger()
workQueue   = Queue()
resultQueue = Queue()

urlNeedingReboot = 'http://build.mozilla.org/builds/slaves_needing_reboot.txt'


_defaultOptions = { 'config':      ('-c', '--config',     None,     'Configuration file'),
                    'debug':       ('-d', '--debug',      False,    'Enable Debug', 'b'),
                    'verbose':     ('-v', '--verbose',    False,    'Verbose output', 'b'),
                    'background':  ('-b', '--background', False,    'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',    None,     'Path where log file is to be written'),
                    'kittens':     ('-k', '--kittens',    None,     'file or url to use as source of kittens'),
                    'filter':      ('-f', '--filter',     None,     'regex filter to apply to list'),
                    'environ':     ('',   '--environ',    'prod',   'which environ to process, defaults to prod'),
                    'workers':     ('-w', '--workers',    '4',      'how many workers to spawn'),
                    'dryrun':      ('',   '--dryrun',     False,    'do not perform any action if True', 'b'),
                    'filterbase':  ('',   '--filterbase', '^%s',    'string to insert filter expression into'),
                    'username':    ('-u', '--username',   'cltbld', 'ssh username'),
                    'password':    ('-p', '--password',   None,     'ssh password'),
                    'cachefile':   ('',   '--cachefile',  None,     'filename to store the "have we touched this kitten before" cache'),
                    'force':       ('',   '--force',      False,    'force processing of a kitten. This ignores the seen cache *AND* SlaveAlloc', 'b'),
                    'tools':       ('',   '--tools',      None,     'path to tools checkout'),
                    'email':       ('-e', '--email',      False,    'send result email', 'b'),
                  }


def sendEmail(data):
    if len(data) > 0:
        rebooted  = []
        recovered = []
        neither   = []
        idle      = []

        for kitten, result in data:
            print len(result), kitten, result
            if len(result) > 0:
                if result['reboot']:
                    rebooted.append(kitten)
                else:
                    if result['recovery']:
                        recovered.append(kitten)
                    else:
                        if result['reachable']:
                            idle.append('%25s %s' % (kitten, ' '.join(result['output'])))
                        else:
                            neither.append(kitten)

        body = ''
        print rebooted
        print recovered
        print neither
        print idle

        if len(rebooted) > 0:
            s = '\r\nrebooted\r\n'
            t = ''
            m = []
            for item in rebooted:
                t += item
                m.append(item)

                if len(t) > 50:
                    s += '%s\r\n' % ', '.join(m)
                    t = ''
                    m = []

            s    += '    %s\r\n' % ', '.join(m)
            body += s

        if len(recovered) > 0:
            s = '\r\nrecovery needed\r\n'
            t = ''
            m = []
            for item in recovered:
                t += item
                m.append(item)

                if len(t) > 50:
                    s += '%s\r\n' % ', '.join(m)
                    t = ''
                    m = []

            s    += '    %s\r\n' % ','.join(m)
            body += s

        if len(idle) > 0:
            body += '\r\nidle\r\n'
            for item in idle:
                body += '%s\r\n' % item

        if len(neither) > 0:
            body += '\r\nbear needs to look into these\r\n    %s\r\n' % ', '.join(neither)

        if len(body) > 0:
            print body

            addr = 'release@mozilla.com'
            msg  = MIMEText(body)

            msg.set_unixfrom('briarpatch')
            msg['To']      = email.utils.formataddr(('RelEng',     addr))
            msg['From']    = email.utils.formataddr(('briarpatch', addr))
            msg['Subject'] = '[briar-patch] idle kittens report'

            server = smtplib.SMTP('localhost')
            server.set_debuglevel(True)
            server.sendmail(addr, [addr], msg.as_string())
            server.quit()

def processKittens(options, jobs, results):
    remoteEnv = releng.remote.RemoteEnvironment(options.tools, options.username, options.password)
    while True:
        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            r = {}
            if job in remoteEnv.slaves:
                if remoteEnv.slaves[job]['environment'] == options.environ:
                    if not remoteEnv.slaves[job]['enabled'] and not options.force:
                        if options.verbose:
                            log.info('%s not enabled, skipping' % job)
                    elif len(remoteEnv.slaves[job]['notes']) > 0 and not options.force:
                        if options.verbose:
                            log.info('%s has a slavealloc notes field, skipping' % job)
                    else:
                        log.info(job)
                        r = remoteEnv.check(job, indent='    ', dryrun=options.dryrun, verbose=options.verbose, reboot=True)
                else:
                    if options.verbose:
                        log.info('%s not in requested environment %s (%s), skipping' % (job, options.environ, remoteEnv.slaves[job]['environment']))
            else:
                if options.verbose:
                    log.error('%s not listed in slavealloc, skipping' % job)

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


if __name__ == "__main__":
    options = initOptions(_defaultOptions)
    initLogs(options, chatty=False)

    logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

    if options.tools is None:
        options.tools = '/builds/tools'

    if options.cachefile is None:
        options.cachefile = os.path.join(options.appPath, 'reaper_seen.dat')

    if options.kittens is None:
        options.kittens = urlNeedingReboot

    if options.filter is not None:
        reFilter = re.compile(options.filterbase % options.filter)
    else:
        reFilter = None

    log.info('Starting')

    # if reFilter is None:
    #     log.error("During this testing phase I'm making it so that --filter is required")
    #     log.error("Please re-run and specify a filter so we don't accidently process all")
    #     log.error("slaves or something silly like that -- thanks (bear)")
    #     sys.exit(1)

    if options.verbose:
        log.info('retrieving list of kittens to wrangle')

    seenCache = loadCache(options.cachefile)
    kittens   = None

    if options.kittens.lower().startswith('http://'):
        # fetch url, and yes, we assume it's a text file
        items = fetchUrl(options.kittens)
        # and then make it iterable
        if items is not None:
            kittens = items.split('\n')
        else:
            kittens = []
    else:
        if os.path.exists(options.kittens):
            kittens = open(options.kittens, 'r').readlines()
        else:
            if ',' in options.kittens:
                kittens = options.kittens.split(',')
            else:
                kittens = []
                kittens.append(options.kittens)

    if kittens is not None:
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

        sendEmail(emailItems)

        if options.verbose:
            log.info('workers should be all done - closing up shop')

        if len(workers) > 0:
            # now lets wait till they are all done
            for p in workers:
                p.terminate()
                p.join()

    writeCache(options.cachefile, seenCache)

    log.info('Finished')

