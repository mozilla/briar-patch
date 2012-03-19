#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" kittenmonitor.py

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
                            default: None
        -r --redis          Redis server connection string
                            default: localhost:6379
           --redisdb        Redis database ID
                            default: 8
        -d --debug          Turn on debug logging
                            default: False

    Sample Configuration file

        { 'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os, sys
import time
import json
import logging
import smtplib
import email.utils

from email.mime.text import MIMEText
from datetime import date, datetime, timedelta

from releng import initOptions, initLogs, dbRedis

log = logging.getLogger()


# build:mozilla-inbound-android-debug:b7de9902160746b3afaa3496b55ec8f3
 # {'product': 'mobile', 
 #  'slave': 'linux-ix-slave36', 
 #  'branch': 'mozilla-inbound', 
 #  'started': '2012-03-14T15:02:11+01:00', 
 #  'platform': 'android-debug', 
 #  'master': 'buildbot-master25', 
 #  'scheduler': 'mozilla-inbound', 
 #  'builduid': 'b7de9902160746b3afaa3496b55ec8f3', 
 #  'revision': 'cb66ae517284fae3162cedeb78a3687fbfa0173d'}
# build:ux_fedora_test-crashtest:8f190cb4a8974adcb2f40e2b5352d7e9
 # {'product': 'firefox', 
 #  'build_url': 'http://stage.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/ux-linux/1331759247/firefox-14.0a1.en-US.linux-i686.tar.bz2', 
 #  'slave': 'talos-r3-fed-024', 
 #  'branch': 'ux', 
 #  'started': '2012-03-14T15:01:49+01:00', 
 #  'pgo_build': 'False', 
 #  'who': 'sendchange-unittest', 
 #  'elapsed': '286', 
 #  'platform': 'linux', 
 #  'finished': '2012-03-14T15:06:35+01:00', 
 #  'master': 'buildbot-master17', 
 #  'scheduler': 'tests-ux-fedora-opt-unittest', 
 #  'builduid': '8f190cb4a8974adcb2f40e2b5352d7e9', 
 #  'revision': 'f55dc14475ff'}

def gatherData(db, dToday, dHour):
    alerts    = []
    dashboard = {}
    kittens   = {}
    builds    = {}
    jobs      = {}

    print 'build:%s.%s' % (dToday, dHour)

    dashboard['jobs']              = 0
    dashboard['starts']            = 0
    dashboard['finishes']          = 0
    dashboard['maxStarts']         = 0
    dashboard['maxStartsKitten']   = ''
    dashboard['maxFinishes']       = 0
    dashboard['maxFinishesKitten'] = ''
    dashboard['minElapsed']        = 99999
    dashboard['minElapsedKitten']  = ''
    dashboard['meanElapsed']       = 0
    dashboard['maxElapsed']        = 0
    dashboard['maxElapsedKitten']  = ''
    dashboard['maxElapsedJobKey']  = ''

    for key in db.smembers('build:%s.%s' % (dToday, dHour)):
        for jobKey in db.smembers(key):
            build          = db.hgetall(jobKey)
            builds[jobKey] = build

            builduid = build['builduid']
            kitten   = build['slave']

            if kitten not in kittens:
                kittens[kitten] = { 'revisions': [],
                                    'jobs':      [],
                                    'elapsed':   [],
                                    'starts':    0,
                                    'finishes':  0,
                                    'results':   [],
                                  }

            if 'started' in build:
                kittens[kitten]['starts']   += 1
            if 'finished' in build:
                kittens[kitten]['finishes'] += 1
            if 'elapsed' in build:
                kittens[kitten]['elapsed'].append((build['elapsed'], jobKey))
            if 'revision' in build:
                kittens[kitten]['revisions'].append(build['revision'])
            if 'results' in build:
                if build['results'] != 'None':
                    kittens[kitten]['results'].append(int(build['results']))

            kittens[kitten]['jobs'].append(jobKey)

            if builduid not in builds:
                builds[builduid] = { 'kittens':  [],
                                     'started':  None,
                                     'finished': None,
                                   }

            builds[builduid]['kittens'].append(build['slave'])

            if 'started' in build:
                builds[builduid]['started'] = build['started']
            if 'finished' in build:
                builds[builduid]['finished'] = build['finished']

            dashboard['jobs'] += 1

    dKey = 'dashboard:%s.%s' % (dToday, dHour)

    dashboard['kittens'] = len(kittens.keys())

    for host in kittens:
        kitten = kittens[host]

        if kitten['starts'] > 50:
            alerts.append((host, 'starts', kitten['starts']))

        dashboard['starts']   += kitten['starts']
        dashboard['finishes'] += kitten['finishes']

        if kitten['starts'] > dashboard['maxStarts']:
            dashboard['maxStarts']       = kitten['starts']
            dashboard['maxStartsKitten'] = host

        if kitten['finishes'] > dashboard['maxFinishes']:
            dashboard['maxFinishes']       = kitten['finishes']
            dashboard['maxFinishesKitten'] = host

        totalElapsed = 0
        nElapsed     = 0
        for e, jobKey in kitten['elapsed']:
            if not jobKey.startswith('job:None'):
                try:
                    elapsed = int(e)
                except:
                    elapsed = 0
                totalElapsed += elapsed
                nElapsed     += 1

                if elapsed > dashboard['maxElapsed']:
                    dashboard['maxElapsed']       = elapsed
                    dashboard['maxElapsedKitten'] = host
                    dashboard['maxElapsedJobKey'] = jobKey
                if dashboard['minElapsed'] > elapsed:
                    print elapsed, dashboard['minElapsed']
                    dashboard['minElapsed']       = elapsed
                    dashboard['minElapsedKitten'] = host
        if nElapsed > 0:
            dashboard['meanElapsed'] = totalElapsed / nElapsed
        else:
            dashboard['meanElapsed'] = 0

    print dKey, dashboard

    if dashboard['jobs'] > 0:
        for key in dashboard:
            db.hset(dKey, key, dashboard[key])

    return alerts


def sendAlertEmail(alerts, options):
    body = '\r\nThe following alerts have been triggered during dashboard monitoring:'

    for host, alert, value in alerts:
        body += '\r\n\r\n%s %s %s this hour' % (host, alert, value)

    log.info('Sending alert email')
    log.debug(body)

    addr = 'release@mozilla.com'
    msg  = MIMEText(body)

    msg.set_unixfrom('briarpatch')
    msg['To']      = email.utils.formataddr(('RelEng',     addr))
    msg['From']    = email.utils.formataddr(('briarpatch', addr))
    msg['Subject'] = '[briar-patch] monitor alert'

    server = smtplib.SMTP('localhost')
    server.set_debuglevel(options.debug)
    server.sendmail(addr, [addr], msg.as_string())
    server.quit()



_defaultOptions = { 'config':  ('-c', '--config',  None,             'Configuration file'),
                    'debug':   ('-d', '--debug',   True,             'Enable Debug', 'b'),
                    'logpath': ('-l', '--logpath', None,             'Path where log file is to be written'),
                    'redis':   ('-r', '--redis',   'localhost:6379', 'Redis connection string'),
                    'redisdb': ('',   '--redisdb', '8',              'Redis database'),
                    'email':   ('-e', '--email',   False,            'send result email', 'b'),
                  }

if __name__ == '__main__':
    options = initOptions(_defaultOptions)
    initLogs(options)

    log.info('Starting')

    db = dbRedis(options)

    tdHour  = timedelta(hours=-1)
    dGather = datetime.now()

    # gatherData(db, '2012-03-15', '02')
    for i in range(0, 3):
        alerts  = gatherData(db, dGather.strftime('%Y-%m-%d'), dGather.strftime('%H'))
        dGather = dGather + tdHour
    
        if i == 0 and len(alerts) > 0 and options.email:
            sendAlertEmail(alerts, options)

    log.info('done')

