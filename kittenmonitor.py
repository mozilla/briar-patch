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
                            default: 10
        -d --debug          Turn on debug logging
                            default: False

    Sample Configuration file

        { 'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import re
import json
import logging
import smtplib
import email.utils

from email.mime.text import MIMEText
from datetime import datetime, timedelta

from boto.ec2 import connect_to_region

from releng import initOptions, initLogs, dbRedis, dbMysql, getPlatform


log        = logging.getLogger()
_keyExpire = 172800 # 2 days in seconds (1 day = 86,400 seconds)

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

def getJobType(build):
    result = 'Build'
    if build['product'] == 'fuzzing':
        result = 'Other'
    else:
        if 'scheduler' in build:
            if (build['scheduler'] == 'jetpack') or (build['scheduler'].startswith('tests-')):
                result = 'Test'
    return result

def gatherData(db, dToday, dHour):
    alerts    = []
    dashboard = {}
    kittens   = {}
    builds    = {}
    jobs      = {}
    platforms = {}

    print 'build:%s.%s' % (dToday, dHour)

    dashboard['jobs']              = 0
    dashboard['jobsBuild']         = 0
    dashboard['jobsTest']          = 0
    dashboard['jobsOther']         = 0
    dashboard['starts']            = 0
    dashboard['finishes']          = 0
    dashboard['collapses']         = 0
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
            build    = db.hgetall(jobKey)
            builduid = build['builduid']
            kitten   = build['slave']

            if ('cn-sea' not in kitten) and ('cb-sea' not in kitten):
                builds[jobKey] = build
                platform       = getPlatform(build)

                if platform not in platforms:
                    platforms[platform] = 0

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

                if build['product'] == 'fuzzing':
                    dashboard['jobsOther'] += 1
                else:
                    if 'scheduler' in build:
                        if (build['scheduler'] == 'jetpack') or (build['scheduler'].startswith('tests-')):
                            dashboard['jobsTest'] += 1
                        else:
                            dashboard['jobsBuild'] += 1
                    else:
                        dashboard['jobsBuild'] += 1

                if 'request_ids' in build:
                    p = len(build['request_ids'].split(','))
                    if p > 1:
                        platforms[platform]    += 1
                        dashboard['collapses'] += 1

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
                    dashboard['minElapsed']       = elapsed
                    dashboard['minElapsedKitten'] = host
        if nElapsed > 0:
            dashboard['meanElapsed'] = totalElapsed / nElapsed
        else:
            dashboard['meanElapsed'] = 0

    if dashboard['jobs'] > 0:
        for key in dashboard:
            db.hset(dKey, key, dashboard[key])

    dKeyQC = 'dashboard:%s.%s:queue_collapses' % (dToday, dHour)
    for key in platforms:
        db.hset(dKeyQC, key.lower(), platforms[key])
    db.hset(dKeyQC, 'total', dashboard['collapses'])

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

def getPendingCounts(options):
    buildermap = db.hgetall('buildermap')
    mysql      = dbMysql(options)
    jobs       = mysql.pendingJobs()
    hasJobs    = {}
    platforms  = {}

    for builder, count in jobs:
        builderPlatform = getPlatform(builder)

        if builderPlatform not in platforms:
            platforms[builderPlatform] = 0
        platforms[builderPlatform] += count

        for instanceType in buildermap.keys():
            if re.match(buildermap[instanceType], builder):
                if instanceType not in hasJobs:
                    hasJobs[instanceType] = 0
                hasJobs[instanceType] += count

    db.hset('pending', 'byinstance', json.dumps(hasJobs))
    db.hset('pending', 'byplatform', json.dumps(platforms))

def awsUpdate(options):
    secrets = json.load(open(options.secrets))
    conn = connect_to_region(options.region,
        aws_access_key_id=secrets['aws_access_key_id'],
        aws_secret_access_key=secrets['aws_secret_access_key'],
    )

    if conn is not None:
        reservations = conn.get_all_instances()

        farm    = 'ec2'
        farmKey = 'farm:%s' % farm
        current = {}
        for reservation in reservations:
            for instance in reservation.instances:
                if 'moz-state' in instance.tags:
                    dNow = datetime.now()
                    ts   = dNow.strftime('%Y-%m-%dT%H:%M:%SZ')

                    currStatus = { 'state':        instance.state,
                                   'id':           instance.id,
                                   'timestamp':    ts,
                                   'farm':         farm,
                                   'image_id':     instance.image_id,
                                   'vpc_id':       instance.vpc_id,
                                   'platform':     instance.platform,
                                   'region':       instance.region.name,
                                   'launchTime':   instance.launch_time,
                                   'instanceType': instance.instance_type,
                                   'ipPrivate':    instance.private_ip_address,
                                 }
                    for tag in instance.tags.keys():
                        currStatus[tag.lower()] = instance.tags[tag]

                    hostKey = '%s:%s:%s' % (farm, currStatus['name'], currStatus['id'])
                    log.debug('%s %s %s' % (hostKey, farmKey, currStatus['moz-state']))

                    db.sadd(farmKey, hostKey)

                    if 'ec2' in currStatus['name'].lower():
                        if farm not in current:
                            current[farm] = []
                        current[farm].append(hostKey)

                        if currStatus['state'] == 'running':
                            db.sadd('%s:active'   % farmKey, hostKey)
                            db.srem('%s:inactive' % farmKey, hostKey)
                        else:
                            db.sadd('%s:inactive' % farmKey, hostKey)
                            db.srem('%s:active'   % farmKey, hostKey)

                    prevStatus = db.hgetall(hostKey)

                    pipe = db._redis.pipeline()
                    if len(prevStatus) > 0:
                        pipe.rpush('%s:history' % hostKey, prevStatus)
                        pipe.ltrim('%s:history' % hostKey, 0, 300)
                    for tag in currStatus:
                        pipe.hset(hostKey, tag, currStatus[tag])
                        pipe.expire(hostKey, _keyExpire)
                    pipe.execute()

        for farm in current.keys():
            for key in db.smembers('%s:active' % farmKey):
                if key not in current[farm]:
                    db.sadd('%s:inactive' % farmKey, key)
                    db.srem('%s:active'   % farmKey, key)


_defaultOptions = { 'config':  ('-c', '--config',  None,             'Configuration file'),
                    'debug':   ('-d', '--debug',   True,             'Enable Debug'),
                    'logpath': ('-l', '--logpath', None,             'Path where log file is to be written'),
                    'redis':   ('-r', '--redis',   'localhost:6379', 'Redis connection string'),
                    'redisdb': ('',   '--redisdb', '10',             'Redis database'),
                    'mysql':   ('',   '--mysql',   None,             "MySql host:port"),
                    'mysqldb': ('',   '--mysqldb', None,             'MySql database'),
                    'email':   ('-e', '--email',   False,            'send result email'),
                    'region':  ('',   '--region' , 'us-west-1',      'EC2 Region'),
                    }

if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)

    log.info('Starting')

    db = dbRedis(options)

    getPendingCounts(options)

    awsUpdate(options)

    options.redisdb = 8 # metrics data

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

