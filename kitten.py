#!/usr/bin/env python

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

from releng import initOptions, initLogs, fetchUrl, runCommand
import releng.remote


log = get_logger()

urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'


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

def msg(header, msg):
    log.info('%10s %s' % (header, msg))

def check(kitten,  options):
    log.info(kitten)
    remoteEnv = releng.remote.RemoteEnvironment(options.tools, options.username, options.password)

    if slaves[kitten]['enabled']:
        s = 'enabled'
    else:
        s = 'DISABLED'
    msg(s, '%s %s' % (slaves[kitten]['pool'], slaves[kitten]['current_master']))

    note = slaves[kitten]['notes']
    if len(note) > 0:
        msg('note', note)

    pinged, output = remoteEnv.ping(kitten)

    if options.verbose:
        for line in output:
            msg('', line)

    if not pinged:
        msg('', 'OFFLINE [%s]' % output[-1])
    else:
        if kitten.startswith('tegra'):
            print 'need to finish tegra bits...'
        else:
            reachable = False
            inactive  = False
            try:
                remoteEnv.setClient(kitten)
                slave     = releng.remote.getSlave(remoteEnv, kitten)
                output    = slave.wait()
                reachable = len(output) > 0
            except:
                log.error('unable to reach %s' % kitten, exc_info=True)

            if reachable:
                tacfiles = slave.find_buildbot_tacfiles()
                if 'buildbot.tac' in tacfiles:
                    msg('', 'tacfile found')
                else:
                    f = False
                    for tac in tacfiles:
                        m = re.match("^buildbot.tac.bug(\d+)$", tac)
                        if m:
                            f = True
                            msg('', 'tacfile disabled by bug %s' % m.group(1))
                    if not f:
                        msg('', 'offline tacfile found: %s' % ','.join(tacfiles))

                output = slave.tail_twistd_log(n=200)
                if 'Stopping factory' in output:
                    msg('', 'slave is not connected')
                if '; slave is ready' in output:
                    msg('', 'slave has connected to a master')
                if 'Stopping factory' in output:
                    msg('', 'slave may be stopped')

                if len(output) > 0:
                    lines = output.split('\n')
                    logTD = None
                    logTS = None
                    for line in reversed(lines):
                        if '[Broker,client]' in line:
                            logTS  = datetime.datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                            logTD  = datetime.datetime.now() - logTS
                            if options.verbose:
                                msg('tail', line)
                            break
                    if logTD is None:
                        inactive = True
                        msg('', 'OFFLINE - unable to calculate last seen')
                    else:
                        msg('', 'last seen %s' % relative(logTD))
                        if logTD.days > 0 or logTD.seconds > 3600:
                            inactive = True


if __name__ == "__main__":
    options = initOptions(_defaultOptions)
    initLogs(options, chatty=False)

    logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

    if options.tools is None:
        options.tools = '/builds/tools'

    log.debug('Starting')

    # grab and process slavealloc list into a simple dictionary
    slaves    = {}
    slavelist = json.loads(fetchUrl('%s/slaves' % urlSlaveAlloc))
    for item in slavelist:
        if item['notes'] is None:
            item['notes'] = ''
        slaves[item['name']] = item

    for kitten in options.args:
        if kitten in slaves:
            check(kitten, options)
        else:
            log.error('%s is not listed in slavealloc' % kitten)

    log.debug('Finished')
