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

import logging

from multiprocessing import get_logger

from releng import initOptions, initLogs, initKeystore, relative
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

    host = remoteEnv.getHost(kitten)

    print s
    print '%12s: %s' % ('farm',       host.farm)
    print '%12s: %s' % ('colo',       info['datacenter'])
    print '%12s: %s' % ('distro',     info['distro'])
    print '%12s: %s' % ('pool',       info['pool'])
    print '%12s: %s' % ('trustlevel', info['trustlevel'])
    print '%12s: %s' % ('master',     info['current_master'])
    print '%12s: %s' % ('fqdn',       host.fqdn)
    print '%12s: %s' % ('PDU?',       host.hasPDU)
    print '%12s: %s' % ('IPMI?',      host.hasIPMI)

    if len(info['notes']) > 0:
        print '%12s: %s' % ('note', info['notes'])

    if not options.info:
        r = remoteEnv.check(host, dryrun=options.dryrun, verbose=options.verbose, indent='    ', reboot=options.reboot)

        for key in ('reachable', 'buildbot', 'tacfile', 'lastseen', 'master'):
            if key in r:
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

        if 'tacfile' in r and r['tacfile'].lower() != 'not found' and 'master' in r:
            if r['master'] is not None and len(r['master']) > 0:
                m = r['master'][0]
            else:
                m = ''

            master         = remoteEnv.findMaster(m)
            current_master = remoteEnv.findMaster(info['current_master'])
            if master is not None and current_master is not None and master['masterid'] != current_master['masterid']:
                print '%12s: current master is different than buildbot.tac master [%s]' % ('error', m)

        if options.reboot:
            if 'reboot' in r and r['reboot']:
                s = '%12s: via ' % 'reboot'
                if 'ipmi' in r and r['ipmi']:
                    s += 'IPMI'
                elif 'pdu' in r and r['pdu']:
                    s += 'PDU'
            else:
                s = '%12s: ' % 'reboot'
                f = False
                if host.hasPDU:
                    if host.rebootPDU():
                        s += 'via PDU'
                        f  = True
                    else:
                        s += 'tried PDU '
                if not f:
                    if host.hasIPMI:
                        if host.rebootIPMI():
                            s += 'via IPMI'
                            f  = True
                        else:
                            s += 'tried IPMI'
                if not f:
                    s += ', FAILED'

            print s

        if options.stop:
            print host.graceful_shutdown()

        if host.isTegra and options.sdcard:
            host.formatSDCard()

_options = { 'reboot': ('-r', '--reboot', False, 'reboot host if required'),
             'info':   ('-i', '--info',   False, 'show passive info only, do not ssh to host'),
             'stop':   ('',   '--stop',   False, 'stop buildbot for host'),
             'sdcard': ('',   '--sdcard', False, 'reformat tegra sdcard'),
           }


if __name__ == "__main__":
    options = initOptions(params=_options)

    initLogs(options, chatty=False, loglevel=logging.ERROR)

    log.debug('Starting')

    initKeystore(options)

    remoteEnv = releng.remote.RemoteEnvironment(options.tools, passive=options.info)

    for kitten in options.args:
        if kitten in remoteEnv.hosts:
            check(kitten)
        else:
            log.error('%s is not listed in slavealloc' % kitten)

    log.debug('Finished')
