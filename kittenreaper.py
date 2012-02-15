#!/usr/bin/env python

""" RelEng IRC Bot

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
                            default: ./rbot.cfg
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
import time
import random
import logging

from multiprocessing import Process, Queue, get_logger, log_to_stderr
from Queue import Empty

from releng import initOptions, initLogs, fetchUrl, runCommand


log         = get_logger()
workQueue   = Queue()
resultQueue = Queue()

_defaultOptions = { 'config':      ('-c', '--config',     None,  'Configuration file'),
                    'debug':       ('-d', '--debug',      True,  'Enable Debug', 'b'),
                    'background':  ('-b', '--background', False, 'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',    None,  'Path where log file is to be written'),
                    'kittens':     ('-k', '--kittens',    None,  'file or url to use as source of kittens'),
                    'workers':     ('-w', '--workers',    4,     'how many workers to spawn'),
                  }


def checkKitten(job):
    log.info('checking kitten %s', job)

def processKittens(options, jobs, results):
    while True:
        try:
            job = jobs.get(False)
        except Empty:
            job = None

        if job is not None:
            checkKitten(job)
            results.put(job)


if __name__ == "__main__":
    options = initOptions(_defaultOptions)
    initLogs(options)

    if options.kittens is None:
        options.kittens = 'http://build.mozilla.org/builds/slaves_needing_reboot.txt'

    log.info('Starting')

    log.info('retrieving list of kittens to wrangle')

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
            kittens = []

    if len(kittens) > 0:
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
                kitten, enabled = item.split(',')
            except:
                kitten = None
                log.error('unable to parse line [%s]' % item, exc_info=True)

            if kitten is not None:
                workQueue.put(kitten)
                results.append(kitten)

        log.info('waiting for workers to finish...')

        while len(results) > 0:
            try:
                result = resultQueue.get(False)
            except Empty:
                result = None

            if result is not None:
                if result in results:
                    results.remove(result)

        log.info('workers should be all done - closing up shop')

        if len(workers) > 0:
            # now lets wait till they are all done
            for p in workers:
                p.terminate()
                p.join()

    log.info('Finished')
