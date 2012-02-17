#!/usr/bin/env python

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

from multiprocessing import Process, Queue, get_logger, log_to_stderr
from Queue import Empty

from releng import initOptions, initLogs, fetchUrl, runCommand
import releng.remote


log         = get_logger()
workQueue   = Queue()
resultQueue = Queue()

urlSlaveAlloc    = 'http://slavealloc.build.mozilla.org/api'
urlNeedingReboot = 'http://build.mozilla.org/builds/slaves_needing_reboot.txt'


_defaultOptions = { 'config':      ('-c', '--config',     None,     'Configuration file'),
                    'debug':       ('-d', '--debug',      False,    'Enable Debug', 'b'),
                    'background':  ('-b', '--background', False,    'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',    None,     'Path where log file is to be written'),
                    'kittens':     ('-k', '--kittens',    None,     'file or url to use as source of kittens'),
                    'filter':      ('-f', '--filter',     None,     'regex filter to apply to list'),
                    'class':       ('',   '--class',      None,     '"class" of kitten to reboot, will be applied before --kittens if present'),
                    'workers':     ('-w', '--workers',    4,        'how many workers to spawn'),
                    'dryrun':      ('',   '--dryrun',     False,    'do not perform any action if True', 'b'),
                    'filterbase':  ('',   '--filterbase', '^%s',    'string to insert filter express into'),
                    'username':    ('-u', '--username',   'cltbld', 'ssh username'),
                    'password':    ('-p', '--password',   None,     'ssh password'),
                    'cachefile':   ('',   '--cachefile',  None,     'filename to store the "have we touched this kitten before" cache'),
                    'force':       ('',   '--force',      False,    'force processing of a kitten even if it is in the seen cache', 'b'),
                    'tools':       ('',   '--tools',      None,     'path to tools checkout'),
                  }


def checkKitten(hostname, remoteEnv, options):
    log.info('checking kitten %s', hostname)
    if not options.dryrun:
        if not hostname.startswith('tegra'):
            remoteEnv.setClient(hostname)

        releng.remote.checkAndReboot(remoteEnv, hostname)

def processKittens(options, jobs, results):
    remoteEnv = releng.remote.RemoteEnvironment(options.tools, options.username, options.password)
    while True:
        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            checkKitten(job, remoteEnv, options)
            results.put(job)

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

    log.info('retrieving list of kittens to wrangle')

    # grab and process slavealloc list into a simple dictionary
    slaves    = {}
    slavelist = json.loads(fetchUrl('%s/slaves' % urlSlaveAlloc))
    for item in slavelist:
        if item['notes'] is None:
            item['notes'] = ''
        slaves[item['name']] = item

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

    if kittens is not None:
        results = []
        workers = []
        for n in range(1, options.workers):
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

                if reFilter is not None and not reFilter.search(kitten):
                    kitten = None
            except:
                kitten = None
                log.error('unable to parse line [%s]' % item, exc_info=True)

            if kitten is not None:
                if kitten in slaves:
                    if not slaves[kitten]['enabled'] and not options.force:
                        log.info('%s is not enabled, skipping' % kitten)
                    elif len(slaves[kitten]['notes']) > 0 and not options.force:
                        log.info('%s has a notes field, skipping' % kitten)
                    else:
                        if kitten in seenCache:
                            if options.force:
                                log.info("%s has been processed within the last hour but is being --force'd" % kitten)
                            else:
                                log.info('%s has been processed within the last hour, skipping' % kitten)
                                kitten = None
                        if kitten is not None:
                            workQueue.put(kitten)
                            results.append(kitten)
                else:
                    log.error('%s is not listed in slavealloc, skipping' % kitten)

        log.info('waiting for workers to finish...')

        while len(results) > 0:
            try:
                result = resultQueue.get(False)
            except Empty:
                result = None

            if result is not None:
                if result in results:
                    results.remove(result)
                    seenCache[result] = datetime.datetime.now()

        log.info('workers should be all done - closing up shop')

        if len(workers) > 0:
            # now lets wait till they are all done
            for p in workers:
                p.terminate()
                p.join()

    writeCache(options.cachefile, seenCache)

    log.info('Finished')
