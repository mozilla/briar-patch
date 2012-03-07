#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" Host check tool

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
           --dryrun         Do not perform any action, just list what would be done
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

from multiprocessing import get_logger, log_to_stderr

from releng import initOptions, initLogs, runCommand
import releng.remote


log = get_logger()


_defaultOptions = { 'config':      ('-c', '--config',     None,     'Configuration file'),
                    'debug':       ('-d', '--debug',      False,    'Enable Debug', 'b'),
                    'background':  ('-b', '--background', False,    'daemonize ourselves', 'b'),
                    'logpath':     ('-l', '--logpath',    None,     'Path where log file is to be written'),
                    'dryrun':      ('',   '--dryrun',     False,    'do not perform any action if True', 'b'),
                    'username':    ('-u', '--username',   'cltbld', 'ssh username'),
                    'password':    ('-p', '--password',   None,     'ssh password'),
                    'force':       ('',   '--force',      False,    'force processing of a kitten even if it is in the seen cache', 'b'),
                    'tools':       ('',   '--tools',      None,     'path to tools checkout'),
                    'verbose':     ('-v', '--verbose',    False,    'show extra output from remote commands', 'b'),
                  }


def check(kitten,  remoteEnv, options):
    s = '%s: ' % kitten
    if remoteEnv.slaves[kitten]['enabled']:
        s += 'enabled'
    else:
        # hopefully short term hack until tegras are
        # handled properly by slavealloc...
        if 'tegra' in kitten:
            s += 'enabled'
        else:
            s += 'DISABLED'

    s += ' %s %s' % (remoteEnv.slaves[kitten]['pool'], remoteEnv.slaves[kitten]['current_master'])

    log.info(s)
    note = remoteEnv.slaves[kitten]['notes']
    if len(note) > 0:
        log.info('    note %s' % note)

    pinged, output = remoteEnv.ping(kitten)
    if not pinged:
        log.info('    OFFLINE [%s]' % output[-1])

    remoteEnv.check(kitten, dryrun=options.dryrun, verbose=options.verbose, indent='    ', reboot=True)


if __name__ == "__main__":
    options = initOptions(_defaultOptions)
    initLogs(options, chatty=False)

    logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

    if options.tools is None:
        options.tools = '/builds/tools'

    log.debug('Starting')

    remoteEnv = releng.remote.RemoteEnvironment(options.tools, options.username, options.password)

    for kitten in options.args:
        if kitten in remoteEnv.slaves:
            check(kitten, remoteEnv, options)
        else:
            log.error('%s is not listed in slavealloc' % kitten)

    log.debug('Finished')
