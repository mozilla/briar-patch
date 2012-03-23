#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" Host check tool

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

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

from multiprocessing import get_logger, log_to_stderr

from releng import initOptions, initLogs, runCommand, initKeystore, relative
import releng.remote


log = get_logger()


def check(kitten,  remoteEnv, options):
    s = '%s: ' % kitten

    if remoteEnv.hosts[kitten]['enabled']:
        s += 'enabled'
    else:
        # hopefully short term hack until tegras are
        # handled properly by slavealloc...
        if 'tegra' in kitten:
            s += 'enabled'
        else:
            s += 'DISABLED'

    print s
    print '%12s: %s' % ('trustlevel', remoteEnv.hosts[kitten]['trustlevel'])
    print '%12s: %s' % ('pool',       remoteEnv.hosts[kitten]['pool'])
    print '%12s: %s' % ('master',     remoteEnv.hosts[kitten]['current_master'])
    print '%12s: %s' % ('distro',     remoteEnv.hosts[kitten]['distro'])
    print '%12s: %s' % ('colo',       remoteEnv.hosts[kitten]['datacenter'])

    note = remoteEnv.hosts[kitten]['notes']
    if len(note) > 0:
        print '%12s: %s' % ('note', note)

    pinged, output = remoteEnv.ping(kitten)
    if not pinged:
        print '%12s: %s' % ('OFFLINE', output[-1])

    if not options.info:
        r = remoteEnv.check(kitten, dryrun=options.dryrun, verbose=options.verbose, indent='    ', reboot=options.reboot)

        for key in r:
            s = r[key]
            if key == 'lastseen':
                if r[key] is None:
                    s = 'unknown'
                else:
                    s = relative(r[key])
            print '%12s: %s' % (key, s)

_options = { 'reboot': ('-r', '--reboot', False, 'reboot host if required', 'b'), 
             'info':   ('-i', '--info',   False, 'show passive info only, do not ssh to host', 'b'), 
           }


if __name__ == "__main__":
    options = initOptions(params=_options)

    initLogs(options, chatty=False, loglevel=logging.ERROR)

    log.debug('Starting')

    initKeystore(options)

    remoteEnv = releng.remote.RemoteEnvironment(options.tools)

    for kitten in options.args:
        if kitten in remoteEnv.hosts:
            check(kitten, remoteEnv, options)
        else:
            log.error('%s is not listed in slavealloc' % kitten)

    log.debug('Finished')
