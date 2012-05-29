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

from multiprocessing import get_logger, log_to_stderr

from releng import initOptions, initLogs, runCommand, initKeystore, relative
import releng.remote


log = get_logger()


def check(kitten):
    s = '%s: ' % kitten

    info = remoteEnv.hosts[kitten]
    if info['enabled']:
        s += 'enabled'
    else:
        # hopefully short term hack until tegras are
        # handled properly by slavealloc...
        if 'tegra' in kitten:
            s += 'enabled'
        else:
            s += 'DISABLED'

    print s
    print '%12s: %s' % ('colo',       info['datacenter'])
    print '%12s: %s' % ('distro',     info['distro'])
    print '%12s: %s' % ('pool',       info['pool'])
    print '%12s: %s' % ('trustlevel', info['trustlevel'])

    if len(info['notes']) > 0:
        print '%12s: %s' % ('note', info['notes'])


    if options.info:
        print '%12s: %s' % ('master', info['current_master'])
    else:
        host = remoteEnv.getHost(kitten)
        r    = remoteEnv.check(host, dryrun=options.dryrun, verbose=options.verbose, indent='    ', reboot=options.reboot)

        for key in ('fqdn', 'reachable', 'buildbot', 'tacfile', 'lastseen', 'master'):
            s = r[key]
            if key == 'lastseen':
                if r[key] is None:
                    s = 'unknown'
                else:
                    s = relative(r[key])
            if key == 'master':
                if r[key] is None:
                    s = info['current_master']
                else:
                    if len(r[key]) > 0:
                        s = r[key][0]
                    else:
                        s = r[key]
            print '%12s: %s' % (key, s)

        if 'master' in r:
            if r['master'] is not None and len(r['master']) > 0:
                m = r['master'][0]
            else:
                m = ''

            master         = remoteEnv.findMaster(m)
            current_master = remoteEnv.findMaster(info['current_master'])
            if master is not None and current_master is not None and master['masterid'] != current_master['masterid']:
                print '%12s: current master is different than buildbot.tac master [%s]' % ('error', m)

        print '%12s: %s' % ('IPMI?', host.hasIPMI)

        if options.stop:
            print host.graceful_shutdown()

_options = { 'reboot': ('-r', '--reboot', False, 'reboot host if required', 'b'),
             'info':   ('-i', '--info',   False, 'show passive info only, do not ssh to host', 'b'),
             'stop':   ('',   '--stop',   False, 'stop buildbot for host', 'b'),
           }


if __name__ == "__main__":
    options = initOptions(params=_options)

    initLogs(options, chatty=False, loglevel=logging.ERROR)

    log.debug('Starting')

    initKeystore(options)

    remoteEnv = releng.remote.RemoteEnvironment(options.tools)

    for kitten in options.args:
        if kitten in remoteEnv.hosts:
            check(kitten)
        else:
            log.error('%s is not listed in slavealloc' % kitten)

    log.debug('Finished')
