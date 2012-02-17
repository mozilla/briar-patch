#!/usr/bin/env python

""" Host Reboot Bug tool

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

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import sys, os
import re
import time
import json
import logging
import datetime
import paramiko

from multiprocessing import get_logger, log_to_stderr

from bugzilla.agents import BMOAgent
from bugzilla.utils import get_credentials

from releng import initOptions, initLogs, fetchUrl
import releng.remote


log = get_logger()

urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'

_reBug = r"""bugs?\s*,?\s*(?:#|nos?\.?|num(?:ber)?s?)?\s*
             ((?:\d+\s*(?:,?\s*(?:and)?)?\s*)+)"""


_defaultOptions = { 'config':   ('-c', '--config',     None,     'Configuration file'),
                    'debug':    ('-d', '--debug',      True,     'Enable Debug', 'b'),
                    'logpath':  ('-l', '--logpath',    None,     'Path where log file is to be written'),
                    'dryrun':   ('',   '--dryrun',     False,    'do not perform any action if True', 'b'),
                    'username': ('-u', '--username',   'cltbld', 'ssh username'),
                    'password': ('-p', '--password',   None,     'ssh password'),
                    'force':    ('',   '--force',      False,    'force processing of a host', 'b'),
                    'tools':    ('',   '--tools',      None,     'path to tools checkout'),
                  }             


def getBugs(host):
    # We can use "None" for both instead to not authenticate
    # username, password = get_credentials()

    # Load our agent for BMO
    bmo = BMOAgent(None, None)

    # Set whatever REST API options we want
    options = {
        'alias': 'reboots-sjc1',
    }

    # Get the bugs from the api
    buglist = bmo.get_bug_list(options)

    print "Found %s bugs" % (len(buglist))

    for bug in buglist:
        print bug.id, bug.status, bug.summary, bug.blocks, bug.depends_on
        print bug.id, bug.comments


def process(host, remoteEnv):
    issues = []

    pinged, output = remoteEnv.ping(host)

    if pinged:
        issues.append('is pingable')

        try:
            remoteEnv.setClient(host)

            slave    = releng.remote.getSlave(remoteEnv, host)
            tacfiles = slave.find_buildbot_tacfiles()
            issues.append('is alive')

            if "buildbot.tac" not in tacfiles:
                log.info("Found these tacfiles: %s", tacfiles)
                for tac in tacfiles:
                    m = re.match("^buildbot.tac.bug(\d+)$", tac)
                    if m:
                        log.info("Disabled by bug %s" % m.group(1))
                        issues.append('m.group(1)')
                log.info("Didn't find buildbot.tac")
                issues.append('no tacfile')
        except:
            log.info('exception during processing of %s - but then again it is *supposed* to be offline' % slave.hostname)

    if len(issues) == 0:
        buglist = getBugs(host)
    else:
        print issues


if __name__ == "__main__":
    options = initOptions(_defaultOptions)
    initLogs(options)

    logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

    if options.tools is None:
        options.tools = '/builds/tools'

    log.info('Starting')

    # grab and process slavealloc list into a simple dictionary
    slaves    = {}
    slavelist = json.loads(fetchUrl('%s/slaves' % urlSlaveAlloc))
    for item in slavelist:
        if item['notes'] is None:
            item['notes'] = ''
        slaves[item['name']] = item

    reBug     = re.compile(_reBug)
    remoteEnv = releng.remote.RemoteEnvironment(options.tools, options.username, options.password)

    for host in options.args:
        flag  = False
        log.info('processing %s' % host)

        if not options.dryrun:
            if slaves[host]['enabled']:
                notes = slaves[host]['notes']
                if len(notes) > 0:
                    if reBug.match(notes):
                        flag = True
                        print "slave has a bug # reference in it's notes field: [%s]" % notes

            else:
                print "slave is not enabled"
                flag = True

        if not flag or options.force:
            process(host, remoteEnv)

    log.info('Finished')
