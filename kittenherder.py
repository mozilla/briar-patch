#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

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

import os
import re
import datetime
import smtplib
import email.utils

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from multiprocessing import get_logger

from boto.ec2 import connect_to_region

from releng import initOptions, initLogs, fetchUrl, dbRedis, initKeystore, relative, getPassword, getPlatform
import releng.remote


log        = get_logger()
_keyExpire = 1209600 # 14 days in seconds (1 day = 86,400 seconds)
_workers   = 1

urlNeedingReboot = 'http://build.mozilla.org/builds/slaves_needing_reboot.txt'


_defaultOptions = { 'kittens':    ('-k', '--kittens',    None,     'farm keyword, list or url to use as source of kittens'),
                    'filter':     ('-f', '--filter',     None,     'regex filter to apply to list'),
                    'environ':    ('',   '--environ',    'prod',   'which environ to process, defaults to prod'),
                    'workers':    ('-w', '--workers',    '1',      'how many workers to spawn'),
                    'filterbase': ('',   '--filterbase', '^%s',    'string to insert filter expression into'),
                    'cachefile':  ('',   '--cachefile',  None,     'filename to store the "have we touched this kitten before" cache'),
                    'force':      ('',   '--force',      False,    'force processing of a kitten. This ignores the seen cache *AND* SlaveAlloc'),
                    'email':      ('-e', '--email',      False,    'send result email'),
                    'redis':      ('-r', '--redis',     'localhost:6379', 'Redis connection string'),
                    'redisdb':    ('',   '--redisdb',   '10',             'Redis database'),
                    'smtpServer': ('',   '--smtpServer', None,     'where to send generated email to'),
                  }


def generateTextList(hostlist, tag, indent=''):
    s = '\r\n%s\r\n' % tag
    t = ''
    m = []
    for item in hostlist:
        t += item
        m.append(item)

        if len(t) > 50:
            s += '%s%s\r\n' % (indent, ', '.join(m))
            t = ''
            m = []

    s += '%s    %s\r\n' % (', '.join(m), indent)

    return s

def previouslySeen(hostlist, lastrun):
    result = ''
    l      = []
    for kitten in lastrun:
        if kitten in hostlist:
            l.append(kitten)
            hostlist.remove(kitten)

    return l

def getHistory(kitten):
    result = ''
    keys   = db.keys('kittenherder:*:%s' % kitten)
    keys.sort(reverse=True)
    for key in keys:
        d = db.hgetall(key)
        indent  = '    %s ' % key.replace('kittenherder:', '').replace(':%s' % kitten, '')
        result += indent

        for f in ('reachable', 'buildbot'):
            if f in d:
                result += '%s: %s ' % (f, d[f])
        result += '\r\n'
        result += ' ' * len(indent)
        for f in ('reboot', 'recovery', 'lastseen'):
            if f in d:
                result += '%s: %s ' % (f, d[f])
        result += '\r\n'

    return result

#        bm-xserve20 {'recovery': True, 'ipmi': False, 'output': ['adding to recovery list because host is not reachable', 'adding to recovery list because last activity is unknown'],
#                     'tacfile': '', 'pdu': False, 'fqdn': 'bm-xserve20.build.sjc1.mozilla.com.', 'reboot': False, 'reachable': False, 'lastseen': None,
#                     'buildbot': '', 'master': ''}

def HTMLEmailHeader(title):
    header =  """<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>"""
    header += title
    header += """</title>
<link rel="stylesheet" href="http://build.mozilla.org/builds/kitten_mail.css" />
</head>

<body>
"""
    header += '<h1>%s</h1>' % title
    return header

def HTMLEmailFooter():
    return """
<hr/>
<p class="center"><a href="http://build.mozilla.org/builds/last-job-per-slave.html">last job per slave</a> | <a href="http://slavealloc.build.mozilla.org/ui/">slavealloc</a></p>

</body>
</html>

"""

def getOS(kitten):
    if 'try-mac64' in kitten or \
       'lion' in kitten or \
       'moz2-darwin10' in kitten or \
       'talos-r3-leopard' in kitten or \
       'talos-r4-snow-' in kitten:
        return 'Mac%20OS%20X'
    elif 'centos' in kitten or \
         'linux' in kitten or \
         'talos-r3-fed' in kitten:
        return 'Linux'
    elif 'w64' in kitten:
        return 'Windows%20Server%202008'
    elif 'w7' in kitten:
        return 'Windows%207'
    elif 'xp' in kitten:
        return 'Windows%20XP'
    elif 'tegra' in kitten:
        return 'Android'
    else:
        return ''

def getTemplateLink(kitten):
    platform = getPlatform(kitten)
    os       = getOS(kitten)
    link = '<a href="https://bugzilla.mozilla.org/enter_bug.cgi?alias=' + kitten + '&assigned_to=nobody%40mozilla.org&bug_severity=normal&bug_status=NEW&component=Release%20Engineering%3A%20Machine%20Management&contenttypemethod=autodetect&contenttypeselection=text%2Fplain&data=&defined_groups=1&flag_type-4=X&flag_type-481=X&flag_type-607=X&flag_type-674=X&flag_type-720=X&flag_type-721=X&flag_type-737=X&flag_type-775=X&flag_type-780=X&form_name=enter_bug&keywords=&maketemplate=Remember%20values%20as%20bookmarkable%20template&op_sys=' + os + '&priority=--&product=mozilla.org&qa_contact=armenzg%40mozilla.com&rep_platform=' + platform + '&requestee_type-4=&requestee_type-607=&requestee_type-753=&short_desc=' + kitten + '%20problem%20tracking&status_whiteboard=%5Bbuildduty%5D%5Bbuildslave%5D%5Bcapacity%5D&version=other">File new bug</a>'
    return link

def formatHTMLResults(table_header, kitten_list):
    results = """
<table cellpadding="0" cellspacing="0" width="620" class="body">
<tr>
"""
    results += '<th colspan="3">%s</th>\n' % table_header
    results += '</tr>\n'

    row_class = 'odd'
    for kitten in kitten_list:        
        results += '<tr class="%s"><td>%s</td>\n' % (row_class,kitten)
        results += '<td><a href="https://bugzilla.mozilla.org/show_bug.cgi?id=%s">Check Existing Bug</a></td>\n' % kitten
        results += '<td>' + getTemplateLink(kitten) + '</td>\n'
        results += '</tr>\n'
        if row_class == 'odd':
            row_class = 'even'
        else:
            row_class = 'odd'
    results += '</table>'
    return results

def addHTMLLineBreak():
    return '<br/>'

def sendEmail(data, smtpServer=None):
    if len(data) > 0:
        rebootedOS   = []
        rebootedIPMI = []
        rebootedPDU  = []
        recovered    = []
        idle         = []
        neither      = []
        body         = ''
        html_body    = ''

        lastRun = db.lrange('kittenherder:lastrun', 0, -1)
        db.ltrim('kittenherder:lastrun', 0, 0)
        db.expire('kittenherder:lastrun', _keyExpire)

        print lastRun
        for kitten, result in data:
            db.lpush('kittenherder:lastrun', kitten)

            print len(result), kitten, result
            if len(result) > 0:
                if result['reboot']:
                    if result['ipmi']:
                        rebootedIPMI.append(kitten)
                    elif result['pdu']:
                        rebootedPDU.append(kitten)
                    else:
                        rebootedOS.append(kitten)
                elif result['recovery']:
                    recovered.append(kitten)
                elif 'idle' in result['buildbot']:
                    idle.append(kitten)
                else:
                    if not result['reachable']:
                        neither.append(kitten)

        if len(idle) > 0:
            body += '\r\nbored kittens\r\n    %s\r\n' % ', '.join(idle)
            html_body += formatHTMLResults('bored kittens', idle)
            html_body += addHTMLLineBreak()
    
        if len(rebootedOS) > 0:
            prevSeen = previouslySeen(rebootedOS, lastRun)
            body += generateTextList(rebootedOS, 'rebooted (SSH)')
            body += generateTextList(prevSeen, 'rebooted (SSH): previously seen', '    ')
            html_body += formatHTMLResults('rebooted (SSH)', rebootedOS)
            html_body += formatHTMLResults('rebooted (SSH): previously seen', prevSeen)
            html_body += addHTMLLineBreak()

        if len(rebootedPDU) > 0:
            prevSeen = previouslySeen(rebootedPDU, lastRun)
            body += generateTextList(rebootedPDU, 'rebooted (PDU)')
            body += generateTextList(prevSeen, 'rebooted (PDU): previously seen', '    ')
            html_body += formatHTMLResults('rebooted (PDU)', rebootedPDU)
            html_body += formatHTMLResults('rebooted (PDU): previously seen', prevSeen)
            html_body += addHTMLLineBreak()

        if len(rebootedIPMI) > 0:
            prevSeen = previouslySeen(rebootedIPMI, lastRun)
            body += generateTextList(rebootedIPMI, 'rebooted (IPMI)')
            body += generateTextList(prevSeen, 'rebooted (IPMI): previously seen', '    ')
            html_body += formatHTMLResults('rebooted (IPMI)', rebootedIPMI) 
            html_body += formatHTMLResults('rebooted (IPMI): previously seen', prevSeen)
            html_body += addHTMLLineBreak()

        if len(recovered) > 0:
            body += '\r\nrecovery needed\r\n'
            for kitten in recovered:
                body += '%s\r\n%s' % (kitten, getHistory(kitten))
            html_body += formatHTMLResults('recovery needed', recovered)
            html_body += addHTMLLineBreak()

        if len(neither) > 0:
            body += '\r\nbear needs to look into these\r\n    %s\r\n' % ', '.join(neither)

        if len(body) > 0:
            addr = 'release@mozilla.com'                                     
            msg = MIMEMultipart('alternative') 

            msg.set_unixfrom('briarpatch')
            msg['To']      = email.utils.formataddr(('RelEng',     addr))
            msg['From']    = email.utils.formataddr(('briarpatch', addr))
            msg['Subject'] = '[briar-patch] idle kittens report'

            textPart = MIMEText(body, 'plain')                                
            htmlPart = MIMEText(HTMLEmailHeader('[briar-patch] idle kittens report') + \
                                html_body + \
                                HTMLEmailFooter(), 'html')

            msg.attach(textPart)                                              
            msg.attach(htmlPart)

            if smtpServer is not None:
                server = smtplib.SMTP(smtpServer)
                server.set_debuglevel(True)
                server.sendmail(addr, [addr], msg.as_string())
                server.quit()

def processKitten(options, remoteEnv, job):
    dNow  = datetime.datetime.now()
    dDate = dNow.strftime('%Y-%m-%d')
    dHour = dNow.strftime('%H')
    r     = {}

    if job is not None:
        if job in remoteEnv.hosts:
            info = remoteEnv.hosts[job]
            if info['environment'] == options.environ:
                if not info['enabled'] and not options.force:
                    if options.verbose:
                        log.info('%s not enabled, skipping' % job)
                elif len(info['notes']) > 0 and not options.force:
                    if options.verbose:
                        log.info('%s has a slavealloc notes field, skipping' % job)
                else:
                    log.info(job)
                    host = remoteEnv.getHost(job)
                    if host is None:
                        log.error('unknown host for %s' % job)
                    else:
                        r = remoteEnv.check(host, indent='    ', dryrun=options.dryrun, verbose=options.verbose)

                        if host.farm != 'ec2':
                            d = remoteEnv.rebootIfNeeded(host, lastSeen=r['lastseen'], indent='    ', dryrun=options.dryrun, verbose=options.verbose)
                            for s in ['reboot', 'recovery', 'ipmi', 'pdu']:
                                r[s] = d[s]
                            r['output'] += d['output']

                        r['host'] = host
                        hostKey   = 'kittenherder:%s.%s:%s' % (dDate, dHour, job)
                        for key in r:
                            db.hset(hostKey, key, r[key])
                        db.expire(hostKey, _keyExpire)

                        # all this because json cannot dumps() the timedelta object
                        td = r['lastseen']
                        if td is not None:
                            secs             = td.seconds
                            hours, remainder = divmod(secs, 3600)
                            minutes, seconds = divmod(remainder, 60)
                            r['lastseen']    = { 'hours':    hours,
                                                 'minutes':  minutes,
                                                 'seconds':  seconds,
                                                 'relative': relative(td),
                                                 'since':    secs,
                                               }
            else:
                if options.verbose:
                    log.info('%s not in requested environment %s (%s), skipping' % (job, options.environ, info['environment']))
        else:
            if options.verbose:
                log.error('%s not listed in slavealloc, skipping' % job, exc_info=True)

    return r

def processEC2(ec2Kittens):
    keynames = db.keys('counts:*')
    counts   = {}

    for item in keynames:
        instanceType         = item.replace('counts:', '')
        counts[instanceType] = { 'current': 0 }

        count        = db.hgetall(item)
        for key in count.keys():
            counts[instanceType][key] = count[key]

    for kitten, r in ec2Kittens:
        host         = r['host']
        instanceType = host.info['class']
        if instanceType not in counts:
            log.error('%s has a instance type [%s] not found in our counts, assuming minimum of 2 and max of 50' % (kitten, instanceType))
            counts[instanceType]['max']     = 50
            counts[instanceType]['min']     = 2
            counts[instanceType]['current'] = 0

        if host.info['enabled'] and host.info['state'] == 'running':
            counts[instanceType]['current'] += 1

        if 'lastseen' in r:
            log.info('%s: count = %d idle: %dh %dm %ss' % (instanceType, counts[instanceType]['current'], r['lastseen']['hours'], r['lastseen']['minutes'], r['lastseen']['seconds']))

            if r['lastseen']['since'] > 3600:
                if host.info['enabled'] and host.info['state'] == 'running':
                    log.info('shutting down ec2 instance')
                    # if we can ssh to host, then try and do normal shutdowns
                    if host.graceful_shutdown():
                        log.info("instance was graceful'd")
                    try:
                        conn = connect_to_region(host.info['region'],
                                                 aws_access_key_id=getPassword('aws_access_key_id'),
                                                 aws_secret_access_key=getPassword('aws_secret_access_key'))
                        conn.stop_instances(instance_ids=[host.info['id'],])
                    except:
                        log.error('unable to stop ec2 instance %s [%s]' % (kitten, host.info['id']), exc_info=True)
                else:
                    log.error('ec2 instance flagged for reboot/recovery but it is not running')

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

def loadKittenList(options):
    result = []

    if options.kittens.lower() in ('ec2',):
        for item in db.smembers('farm:%s:active' % options.kittens):
            itemName = db.hget(item, 'name')
            if itemName is None:
                log.info('Skipping bad entry [%s]' % item)
            else:
                result.append(db.hget(item, 'name'))

    elif options.kittens.lower().startswith('http://'):
        # fetch url, and yes, we assume it's a text file
        items = fetchUrl(options.kittens)
        # and then make it iterable
        if items is not None:
            result = items.split('\n')

    elif os.path.exists(options.kittens):
        result = open(options.kittens, 'r').readlines()

    elif ',' in options.kittens:
        result = options.kittens.split(',')
    else:
        result.append(options.kittens)

    return result


if __name__ == "__main__":
    options = initOptions(params=_defaultOptions)

    initLogs(options, chatty=False)

    if options.cachefile is None:
        options.cachefile = os.path.join(options.appPath, 'kittenherder_seen.dat')

    if options.kittens is None:
        log.info('kitten list not specified, defaulting to %s' % urlNeedingReboot)
        options.kittens = urlNeedingReboot

    if options.filter is not None:
        reFilter = re.compile(options.filterbase % options.filter)
    else:
        reFilter = None

    db = dbRedis(options)

    log.info('Starting')

    initKeystore(options)

    if options.verbose:
        log.info('retrieving list of kittens to wrangle')

    emailItems = []
    ec2Kittens = []
    seenCache  = loadCache(options.cachefile)
    kittens    = loadKittenList(options)
    remoteEnv  = releng.remote.RemoteEnvironment(options.tools, db=db)

    if len(kittens) > 0:
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

                if reFilter is not None and reFilter.search(kitten) is None:
                    log.debug('%s rejected by filter' % kitten)
                    kitten = None
            except:
                kitten = None
                log.error('unable to parse line [%s]' % item, exc_info=True)

            if kitten is not None:
                if kitten in seenCache:
                    if options.force:
                        log.info("%s has been processed within the last hour but is being --force'd" % kitten)
                    else:
                        log.info('%s has been processed within the last hour, skipping' % kitten)
                        kitten = None
                if kitten is not None:
                    r = processKitten(options, remoteEnv, kitten)

                    if 'host' in r and r['host'].farm == 'ec2':
                        ec2Kittens.append((kitten, r))

                    emailItems.append((kitten, r))
                    seenCache[kitten] = datetime.datetime.now()

        processEC2(ec2Kittens)

        if options.email:
            sendEmail(emailItems, options.smtpServer)

    writeCache(options.cachefile, seenCache)

    log.info('Finished')

