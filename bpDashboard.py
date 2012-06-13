#!/usr/bin/env python

""" bpDashboard

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
        -l --logpath        Path where the log file output is written
                            default: None

    Sample Configuration file

        { 'redis': 'localhost:6379',
          'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os
import json

from datetime import datetime, timedelta
from multiprocessing import get_logger

from releng import initOptions, initLogs, dbRedis, fetchUrl


log           = get_logger()
urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'

# 0 - SUCCESS   -  -1 to problem_score
# 1 - WARNINGS  -  +1 to problem_score
# 2 - FAILURE   -  +2 to problem_score
# 4 - EXCEPTION -  +5 to problem_score
# 5 - RETRY     -  +5 to problem_score
weights = { '0': -1, '1': 1, '2': 2, '4': 5, '5': 5 }

pools       = {}
datacenters = {}
masters     = {}
slaves      = {}

BP_SA_CACHE             = 'bp_slavealloc'
BP_SA_CACHE_POOLS       = '%s_pools.json'       % BP_SA_CACHE
BP_SA_CACHE_DATACENTERS = '%s_datacenters.json' % BP_SA_CACHE
BP_SA_CACHE_MASTERS     = '%s_masters.json'     % BP_SA_CACHE
BP_SA_CACHE_SLAVES      = '%s_slaves.json'      % BP_SA_CACHE

def mastersPage(db):
    activityTimes = {}
    buildMasters  = []
    tryMasters    = []
    testMasters   = []
    otherMasters  = []
    dMasters      = { 'try': 0,   'try_active': 0,
                      'build': 0, 'build_active': 0,
                      'test': 0,  'test_active': 0,
                      'other': 0, 'other_active': 0,
                    }

    dNow  = datetime.now()
    dDate = dNow.strftime('%Y-%m-%d')

    activityTimes[dDate] = {}

    activity      = {}
    activeSlaves  = {}
    activeMasters = {}
    countMasters  = {}

    activityTimes[dDate]['activity'] = activity
    activityTimes[dDate]['slaves']   = activeSlaves
    activityTimes[dDate]['masters']  = activeMasters
    activityTimes[dDate]['counts']   = countMasters

    l = db.smembers('build:%s' % dDate)

    # {'product': 'mobile',
    #  'slave': 'tegra-104',
    #  'started': '2012-02-03T18:28:25-08:00',
    #  'finished': '2012-02-03T18:32:34-08:00',
    #  'master': 'buildbot-master20',
    #  'branch': 'try',
    #  'builduid': 'f4c87f500a75499492c0672c6100e4ed',
    #  'revision': '93765d250bf6'}

    for buildKey in l:
        build      = db.hgetall(buildKey)
        slaveName  = build['slave']
        masterName = build['master']

        activity[buildKey] = build
        if slaveName in slaves:
            slaves[slaveName]['activity'].append(buildKey)
        if slaveName not in activeSlaves:
            activeSlaves[slaveName] = []
        activeSlaves[slaveName].append(buildKey)

        for keyMaster in masters:
            if masterName in masters[keyMaster]['fqdn']:
                if masterName not in activeMasters:
                    activeMasters[masterName] = []
                activeMasters[masterName].append(buildKey)
                break

        if 'elapsed' not in build:
            if 'started' in build and 'finished' in build:
                dStarted  = datetime.strptime(build['started'][:-6],  '%Y-%m-%dT%H:%M:%S')
                dFinished = datetime.strptime(build['finished'][:-6], '%Y-%m-%dT%H:%M:%S')
                tdElapsed = dFinished - dStarted
                build['elapsed'] = '%d' % ((tdElapsed.days * 86400) + tdElapsed.seconds)
            else:
                build['elapsed'] = '0'

        if 'started' not in build:
            build['started'] = ''
        if 'finished' not in build:
            build['finished'] = ''

    for keyMaster in masters:
        master = masters[keyMaster]
        pool   = master['pool']
        if keyMaster == 'bm04-tests1-macosx':
            print '***********************************'
            print pool, master
        countMasters[keyMaster] = { 'total_slaves':  0,
                                    'active_slaves': 0,
                                  }

        if 'try-' in pool:
            tryMasters.append(keyMaster)
            dMasters['try'] += 1
            if master['enabled']:
                dMasters['try_active'] += 1
        elif 'tests-' in pool:
            testMasters.append(keyMaster)
            dMasters['test'] += 1
            if master['enabled']:
                dMasters['test_active'] += 1
        elif 'build-' in pool:
            buildMasters.append(keyMaster)
            dMasters['build'] += 1
            if master['enabled']:
                dMasters['build_active'] += 1
        else:
            otherMasters.append(keyMaster)
            dMasters['other'] += 1
            if master['enabled']:
                dMasters['other_active'] += 1

        for keySlave in slaves:
            slave = slaves[keySlave]

            if keyMaster == slave['current_master']:
                countMasters[keyMaster]['total_slaves'] += 1

                for buildKey in slave['activity']:
                    if buildKey in master['activity']:
                        countMasters[keyMaster]['active_slaves'] += 1
                        break

    dMasters['total']        = dMasters['build'] + dMasters['test'] + dMasters['try'] + dMasters['other']
    dMasters['total_active'] = dMasters['build_active'] + dMasters['test_active'] + dMasters['try_active'] + dMasters['other_active']

    h = open('html/masters.html', 'w+')
    h.write("""<div class="header">
<h1>Masters</h1>
<table>
<tr><th></th><th>Build</th><th>Test</th><th>Try</th><th>Other</th><th>Total</th></tr>
<tr><th>Active</th><td>%(build_active)d</td><td>%(test_active)d</td><td>%(try_active)d</td><td>%(other_active)d</td><td>%(total_active)d</td></tr>
<tr><th>Total</th><td>%(build)d</td><td>%(test)d</td><td>%(try)d</td><td>%(other)d</td><td>%(total)d</td></tr>
</table>""" % dMasters)

    hMaster = """<div class="production"><h2>%s Masters</h2>
<table><tr>
<th>%s</th><th colspan="5">%s</th></tr>
<th></th><th colspan="2">Slaves</th><th>Jobs</th><th colspan="3">Elapsed</th>
<th>Master</th><th>Active</th><th>Total</th><th>Best</th><th>Worst</th><th>Avg</th>
</tr>
"""

    h.write(hMaster % ('Build', 'Build', dDate))

    buildMasters.sort()
    for key in buildMasters:
        master = masters[key]

        masterActivity = activityTimes[dDate]['masters']
        elapsedBest  = 999999
        elapsedWorst = 0
        elapsedTotal = 0
        n            = 0
        for buildKey in masterActivity:
            elapsed = activity[buildKey]['elapsed']
            if elapsed < elapsedBest:
                elapsedBest = elapsed
            if elapsed > elapsedWorst:
                elapsedWorst = elapsed
            elapsedTotal += elapsed
            n            += 1
        master['elapsed_best']  = elapsedBest
        master['elapsed_worst'] = elapsedWorst
        if n > 0:
            master['elapsed_avg'] = elapsedTotal / n
        else:
            master['elpased_avg'] = 0

        h.write("""<tr><td><a href="http://%(fqdn)s:%(http_port)s/">%(nickname)s</a></td>
    <td>%(active_slaves)d</td>
    <td>%(total_slaves)d</td>
    <td>%(elapsed_best)d</td>
    <td>%(elapsed_worst)d</td>
    <td>%(elapsed_avg)d</td>
    <td>%(notes)s</td>
</tr>""" % master)
    h.write('</table>\n')

        # {'product': 'mobile',
        #  'slave': 'tegra-104',
        #  'started': '2012-02-03T18:28:25-08:00',
        #  'finished': '2012-02-03T18:32:34-08:00',
        #  'master': 'buildbot-master20',
        #  'branch': 'try',
        #  'builduid': 'f4c87f500a75499492c0672c6100e4ed',
        #  'revision': '93765d250bf6'}

    hJobs = """<h3>Jobs</h3>
<table><tr><th>Build</th><th>Product</th><th>Revision</th><th>Started</th><th>Finished</th><th>Elapsed</th><th>Slave</th></tr>
"""
    dJobs = """<tr><td>%(scheduler)s</td><td>%(product)s</td><td>%(revision)s</td><td>%(started)s</td><td>%(finished)s</td><td>%(elapsed)s</td><td><a href="%(slave)s.html">%(slave)s</a></td></tr>
"""
    h.write(hJobs)
    for key in buildMasters:
        master = masters[key]
        for uid in master['activity']:
            h.write(dJobs % activity[uid])
    h.write('</table>\n</div>\n')

    h.write(hMaster % ('Test', 'Test', dDate))
    testMasters.sort()
    for key in testMasters:
        master = masters[key]
        h.write("""<tr><td><a href="http://%(fqdn)s:%(http_port)s/">%(nickname)s</a></td>
    <td>%(active_slaves)d</td>
    <td>%(total_slaves)d</td>
    <td>%(notes)s</td>
</tr>""" % master)
    h.write('</table>\n</div>\n')

    h.write(hJobs)
    for key in testMasters:
        master = masters[key]
        for uid in master['activity']:
            h.write(dJobs % activity[uid])
    h.write('</table>\n</div>\n')


    h.write(hMaster % ('Try', 'Try', dDate))
    tryMasters.sort()
    for key in tryMasters:
        master = masters[key]
        print key, master
        h.write("""<tr><td><a href="http://%(fqdn)s:%(http_port)s/">%(nickname)s</a></td>
    <td>%(active_slaves)d</td>
    <td>%(total_slaves)d</td>
    <td>%(notes)s</td>
</tr>""" % master)
    h.write('</table>\n</div>\n')

    h.write(hJobs)
    for key in tryMasters:
        master = masters[key]
        for uid in master['activity']:
            h.write(dJobs % activity[uid])
    h.write('</table>\n</div>\n')

    h.write(hMaster % ('Other', 'Other', dDate))
    otherMasters.sort()
    for key in otherMasters:
        master = masters[key]
        h.write("""<tr><td><a href="http://%(fqdn)s:%(http_port)s/">%(nickname)s</a></td>
    <td>%(active_slaves)d</td>
    <td>%(total_slaves)d</td>
    <td>%(notes)s</td>
</tr>""" % master)
    h.write('</table>\n</div>\n')

    h.write(hJobs)
    for key in otherMasters:
        master = masters[key]
        for uid in master['activity']:
            h.write(dJobs % activity[uid])
    h.write('</table>\n</div>\n')

    h.close()


def loadSAdata():
    try:
        if os.path.exists(BP_SA_CACHE_POOLS):
            log.info('loading SlaveAlloc pools data from cache')
            j = json.load(open(BP_SA_CACHE_POOLS))
        else:
            log.info('loading SlaveAlloc pools data')
            j = json.loads(fetchUrl('%s/pools' % urlSlaveAlloc))
            json.dump(j, open(BP_SA_CACHE_POOLS, 'w+'))
        for pool in j:
            pools[pool['poolid']] = pool['name']
    except:
        log.error('Error loading SlaveAlloc pools', exc_info=True)

    try:
        if os.path.exists(BP_SA_CACHE_DATACENTERS):
            log.info('loading SlaveAlloc datacenters data from cache')
            j = json.load(open(BP_SA_CACHE_DATACENTERS))
        else:
            log.info('loading SlaveAlloc datacenters data')
            j = json.loads(fetchUrl('%s/datacenters' % urlSlaveAlloc))
            json.dump(j, open(BP_SA_CACHE_DATACENTERS, 'w+'))
        for dc in j:
            datacenters[dc['dcid']] = dc['name']
    except:
        log.error('Error loading SlaveAlloc datacenters', exc_info=True)

    try:
        if os.path.exists(BP_SA_CACHE_MASTERS):
            log.info('loading SlaveAlloc masters data from cache')
            j = json.load(open(BP_SA_CACHE_MASTERS))
        else:
            log.info('loading SlaveAlloc masters data from cache')
            j = json.loads(fetchUrl('%s/masters' % urlSlaveAlloc))
            json.dump(j, open(BP_SA_CACHE_MASTERS, 'w+'))
        for master in j:
            if master['notes'] is None:
                master['notes'] = ''
            master['activity']          = []
            masters[master['nickname']] = master
    except:
        log.error('Error loading SlaveAlloc masters', exc_info=True)

    try:
        if os.path.exists(BP_SA_CACHE_SLAVES):
            log.info('loading SlaveAlloc slaves data from cache')
            j = json.load(open(BP_SA_CACHE_SLAVES))
        else:
            log.info('loading SlaveAlloc slaves data')
            j = json.loads(fetchUrl('%s/slaves' % urlSlaveAlloc))
            json.dump(j, open(BP_SA_CACHE_SLAVES, 'w+'))
        for slave in j:
            slave['activity']     = []
            slaves[slave['name']] = slave
    except:
        log.error('Error loading SlaveAlloc pools', exc_info=True)


hHeader = """<html>
<head>
    <title>BuildDuty Dashboard v0.1</title>
    <link rel="stylesheet" href="./jquery.tablesorter/themes/green/style.css" type="text/css" media="print, projection, screen" />
    <script type="text/javascript" src="./jquery-1.7.2.min.js"></script>
    <script type="text/javascript" src="./jquery.tablesorter/jquery.tablesorter.js"></script>
    <script type="text/javascript">
    $(function() {
        $("#jobslist").tablesorter({sortList:[[4,0],[0,0]], widgets: ["zebra"]});
    });
    </script>
    <style type="text/css">
        html {
          margin: 10px;
        }
        body {
          font-family: sans-serif;
          font-size: 0.8em;
        }
        h1, h2, h3{
            font-family: Cambria, serif;
            font-size: 2.0em;
            font-style: normal;
            font-weight: normal;
            text-transform: normal;
            letter-spacing: normal;
            line-height: 1.3em;
        }
        #production {
            margin-left: 5%;
        }
        table {
          border: 1px #c4c4c4 solid;
        }
        th {
          background-color: #ccc;
        }
        tr:nth-child(2n-1) {
          background-color: #ccc;
        }
        td {
          padding: 5px;
        }
        a {
          text-decoration: none;
        }
    </style>
</head>
<body>
"""

def secsToDHM(dashboard, key):
    if key in dashboard:
        sec = dashboard[key]
    else:
        sec = '0'
    try:
        seconds = int(sec)
    except:
        seconds = 0

    td  = timedelta(seconds=seconds)
    dhm = datetime(1,1,1) + td
    s   = ''
    d   = dhm.day - 1
    if d > 0:
        s += '%d day' % d
        if d > 1:
            s += 's '
        else:
            s += ' '
    if dhm.hour > 0:
        s += '%d hr' % dhm.hour
        if dhm.hour > 1:
            s += 's '
        else:
            s += ' '
    if dhm.minute > 0:
        s += '%d min' % dhm.minute
        if dhm.minute > 1:
            s += 's '
        else:
            s += ' '

    return s


tHeader = """<table>
<tr><th>Date</th><th colspan="7">Jobs</th><th>Hosts</th><th colspan="2">Elapsed</th></tr>
<tr><th></th><th>Starts</th><th>Finishes</th><th>Collapses</th><th>Build</th><th>Test</th><th>Other</th><th>Total</th><th></th><th>max</th><th>mean</th></tr>
"""
tDetail = """<tr><td><a href="%(hour)s.html">%(date)s %(hour)s</a></td><td>%(starts)s</td><td>%(finishes)s</td><td>%(collapses)s</td><td>%(jobsBuild)s</td><td>%(jobsTest)s</td><td>%(jobsOther)s</td><td>%(jobs)s</td><td>%(kittens)s</td><td><a href="http://%(master_fqdn)s:%(master_port)s/buildslaves/%(maxElapsedKitten)s" title="%(maxElapsedJobKey)s">%(maxElapsed_dhm)s</a></td><td>%(meanElapsed_dhm)s</td></tr>
"""
tFooter = """</table>
"""
kHeader = """<h1>Kitten Summary for the last hour</h1>
<p><strong>Note</strong>: The following tables are sorted by the calculated Results Score column and only include production hosts.  This value is calculated by
generating a weighted score from the finished jobs for the hour:</p>
<ul><li>0 - SUCCESS: -1</li>
    <li>1 - WARNINGS: +1</li>
    <li>2 - FAILURE: +2</li>
    <li>4 - EXCEPTION: +5</li>
    <li>5 - RETRY: +5</li>
</ul>
"""
kTableHeader = """<h2>%s</h2>
<table>
<tr><th>Kitten</th><th colspan="4">Jobs</th>                              <th colspan="3">Results</th>             <th colspan="2">Jobs</th></tr>
<tr><th></th>      <th>Build</th><th>Test</th><th>Other</th><th>Total</th><th>Build</th><th>Test</th><th>Other</th><th>Starts</th><th>Finishes</th></tr>
"""
kTableDetail = """<tr><td><a href="http://%(master_fqdn)s:%(master_port)s/buildslaves/%(kitten)s">%(kitten)s</a></td><td>%(jobsBuild)d</td><td>%(jobsTest)d</td><td>%(jobsOther)d</td><td>%(jobs)d</td>
<td><a name="%(kitten)s_build" title="%(sbResults)s" >%(wbResults)d</a></td>
<td><a name="%(kitten)s_test"  title="%(stResults)s" >%(wtResults)d</a></td>
<td><a name="%(kitten)s_other" title="%(soResults)s" >%(woResults)d</a></td>
<td>%(starts)d</td><td>%(finishes)d</td></tr>
"""
kTableFooter = """</table>
"""
hrHeader = """<h3>Jobs</h3>
<table id="jobslist" class="tablesorter" border="0" cellpadding="0" cellspacing="1">
  <thead>
  <tr><th>Build</th>
      <th>Product</th>
      <th>Branch</th>
      <th>Revision</th>
      <th>Started</th>
      <th>Finished</th>
      <th>Elapsed</th>
      <th>Job Type</th>
      <th>Result</th>
      <th>Slave</th>
  </tr>
  </thead>
  <tbody>
"""
hrDetail = """<tr><td>%(scheduler)s</td><td>%(product)s</td><td>%(branch)s</td><td>%(revision)s</td><td>%(started)s</td><td>%(finished)s</td><td>%(elapsed_dhm)s</td><td>%(jobtype)s</td><td>%(results)s</td><td><a href="%(slave)s.html">%(slave)s</a></td></tr>
"""
hrFooter = """</tbody></table>"""
# {'started': '2012-03-26T05:41:57+01:00', 'product': 'firefox', 'slave': 'mw32-ix-slave18', 'branch': 'try',
#  'log_url': 'http://ftp.mozilla.org/pub/mozilla.org/firefox/try-builds/jacek@codeweavers.com-b0833861ede4/try-win32-debug/try-win32-debug-bm09-try1-build3624.txt.gz',
#  'buildid': '20120326054210', 'who': 'jacek@codeweavers.com', 'results': '0', 'elapsed': '9106',
#  'buildnumber': '3624', 'platform': 'win32-debug', 'finished': '2012-03-26T08:13:43+01:00', 'statusdb_id': '10373031', 'master': 'buildbot-master09',
#  'scheduler': 'try', 'builduid': '59ae60321add4221a3fc4e32b06b0b9e', 'revision': 'b0833861ede4d859cfe465f8db854f52816ae702'}

def getJobType(build):
    result = 'Build'
    if build['product'] == 'fuzzing':
        result = 'Other'
    else:
        if 'scheduler' in build:
            if (build['scheduler'] == 'jetpack') or (build['scheduler'].startswith('tests-')):
                result = 'Test'
    return result

def calculateWeightedScore(results):
    w = 0
    s = ''
    for r in results:
        s += '%s, ' % r
        if r in weights:
            w += weights[r]
    return w, s

def indexPage(db):
    h = open('html/bp.html', 'w+')
    h.write(hHeader)
    h.write(tHeader)

    tdHour = timedelta(hours=-1)
    dNow   = datetime.now()

    #
    # Loop thru and build up the overview table
    #   contains highlight info for the past 6 hours
    #
    for i in range(0, 6):
        dDate = dNow.strftime('%Y-%m-%d')
        dHour = dNow.strftime('%H')

        dashboard = db.hgetall('dashboard:%s.%s' % (dDate, dHour))

        dashboard['date'] = dDate
        dashboard['hour'] = dHour

        # for the first bit of the hour, dashboard doesn't exist in redis
        for s in ('kittens', 'starts', 'finishes', 'master_fqdn', 'master_port',
                  'jobs', 'jobsBuild', 'jobsTest', 'jobsOther', 'collapses',
                  'maxElapsedJobKey', 'maxElapsedKitten',
                 ):
            if s not in dashboard:
                dashboard[s] = ''

        for s in ('minElapsed', 'maxElapsed', 'meanElapsed'):
            dashboard['%s_dhm' % s] = secsToDHM(dashboard, s)

        if 'maxElapsedKitten' in dashboard:
            if dashboard['maxElapsedKitten'] in slaves:
                master = slaves[dashboard['maxElapsedKitten']]['current_master']
                dashboard['master_fqdn'] = masters[master]['fqdn']
                dashboard['master_port'] = masters[master]['http_port']

        h.write(tDetail % dashboard)

        hHr = open('html/%s.html' % dHour, 'w+')
        hHr.write(hHeader)
        hHr.write(hrHeader)

        for key in db.smembers('build:%s.%s' % (dDate, dHour)):
            for jobKey in db.smembers(key):
                build = db.hgetall(jobKey)
                build['elapsed_dhm'] = secsToDHM(build, 'elapsed')
                build['jobtype']     = getJobType(build)

                for s in ('scheduler', 'started', 'finished', 'results'):
                    if s not in build:
                        build[s] = ''
                hHr.write(hrDetail % build)

        hHr.write(hrFooter)
        hHr.close()

        dNow = dNow + tdHour

    h.write(tFooter)
    h.write("""<p><small>Generated %s</small></p>""" % datetime.now().strftime('%Y-%m-%d at %H:%M:%s'))


    # dashboard:2012-04-18.19:queue_collapses
    # {'build/None': 0, 'test/xp': 79, 'build/win32': 0, 'build/linux64-debug': 0, 'build/linuxqt': 0,
    #  'test/w764': 0, 'test/fedora': 59, 'build/win32-debug': 0, 'test/tegra': 98, 'build/b2g': 0,
    #  'test/none': 0, 'test/snowleopard': 7, 'test/fedora64': 54, 'build/linux-debug': 0, 'build/win64': 0,
    #  'build/macosx': 0, 'test/win7': 13, 'build/linux': 0, 'build/linux64': 0, 'build/android': 0,
    #  'build/android-xul': 0, 'build/macosx64': 0, 'build/android-debug': 0, 'build/macosx-debug': 0,
    #  'build/macosx64-debug': 0, 'build/none': 0, 'test/leopard': 43, 'test/macosx': 10, 'test/lion': 8}

    #
    # Loop thru and build up the overview table
    #   contains highlight info for the past 6 hours
    #
    qcPlatforms = []
    qcKeys      = []
    qcData      = {}
    dNow        = datetime.now()
    nHours      = 6

    for i in range(0, nHours):
        dDate = dNow.strftime('%Y-%m-%d')
        dHour = dNow.strftime('%H')

        qcKey = '%s.%s' % (dDate, dHour)
        qc    = db.hgetall('dashboard:%s:queue_collapses' % qcKey)
        qcKeys.append(qcKey)

        qcData[qcKey] = {'date': dDate, 'hour': dHour}
        print qcKey, qc

        for key in qc:
            platform = key.lower()
            try:
                n = int(qc[platform])
            except:
                n = 0

            if n > 0:
                qcData[qcKey][platform] = n
                if platform not in qcPlatforms:
                    qcPlatforms.append(platform)

        for platform in qcPlatforms:
            if platform not in qcData[qcKey]:
                qcData[qcKey][platform] = 0

        dNow = dNow + tdHour

    h.write("\n\n<h1>Queue Collapses for the last %d hours</h1>\n\n<table>" % nHours)
    s = "<tr><th>Platform</th>"
    for key in qcKeys:
        s += "<th>%s</th>" % key
    s += "</tr>\n"
    h.write(s)

    qcPlatforms.sort()
    for platform in qcPlatforms:
        n = 0
        s = "<tr><td>%s</td>" % platform
        for key in qcKeys:
            if platform in qcData[key]:
                n += qcData[key][platform]
                s += "<td>%s</td>" % qcData[key][platform]
            else:
                s += "<td></td>"
        s += "</tr>\n"
        if n > 0:
            h.write(s)
    h.write("</table>\n\n")

    #
    # generate a highlight list of all kittens who need
    # attention for the current hour and the prior hour
    #
    kittens = {}
    dNow    = datetime.now()

    for i in range(0, 2):
        dDate = dNow.strftime('%Y-%m-%d')
        dHour = dNow.strftime('%H')

        for key in db.smembers('build:%s.%s' % (dDate, dHour)):
            for jobKey in db.smembers(key):
                build    = db.hgetall(jobKey)
                # builduid = build['builduid']
                kitten   = build['slave']
                jobType  = getJobType(build)

                if kitten not in kittens:
                    kittens[kitten] = { 'kitten':    kitten,
                                        'Results':   { 'Build': [], 'Test': [], 'Other': [] },
                                        'wResults':  0,
                                        'sResults':  '',
                                        'starts':    0,
                                        'finishes':  0,
                                        'jobs':      0,
                                        'jobsBuild': 0,
                                        'jobsTest':  0,
                                        'jobsOther': 0,
                                        'builds':    [],
                                      }

                kittens[kitten]['jobs'] += 1
                kittens[kitten]['builds'].append((jobKey, build))

                kittens[kitten]['jobs%s' % jobType] += 1

                if 'results' in build:
                    kittens[kitten]['Results'][jobType].append(build['results'])
                if 'started' in build:
                    kittens[kitten]['starts']   += 1
                if 'finished' in build:
                    kittens[kitten]['finishes'] += 1

                if kitten in slaves:
                    master = slaves[kitten]['current_master']
                    if master is None:
                        kittens[kitten]['master_fqdn'] = ''
                        kittens[kitten]['master_port'] = ''
                    else:
                        kittens[kitten]['master_fqdn'] = masters[master]['fqdn']
                        kittens[kitten]['master_port'] = masters[master]['http_port']
                else:
                    kittens[kitten]['master_fqdn'] = ''
                    kittens[kitten]['master_port'] = ''

        dNow = dNow + tdHour

    wbKittens = []
    wtKittens = []
    for host in kittens:
        kitten = kittens[host]

        if host in slaves and (slaves[host]['environment'] == 'prod'):
            w, s = calculateWeightedScore(kitten['Results']['Build'])
            kitten['wbResults'] = w
            kitten['sbResults'] = s
            wbKittens.append((host, w))

            w, s = calculateWeightedScore(kitten['Results']['Test'])
            kitten['wtResults'] = w
            kitten['stResults'] = s
            wtKittens.append((host, w))

            w, s = calculateWeightedScore(kitten['Results']['Other'])
            kitten['woResults'] = w
            kitten['soResults'] = s

    h.write(kHeader)
    h.write(kTableHeader % 'Build')
    swbKittens = sorted(wbKittens, reverse=True, key=lambda k: k[1])
    for host, weight in swbKittens[:10]:
        h.write(kTableDetail % kittens[host])
    h.write(kTableFooter)

    h.write(kTableHeader % 'Tests')
    swtKittens = sorted(wtKittens, reverse=True, key=lambda k: k[1])
    for host, weight in swtKittens[:10]:
        h.write(kTableDetail % kittens[host])
    h.write(kTableFooter)

    h.close()


_defaultOptions = { 'config':  ('-c', '--config',  None,             'Configuration file'),
                    'debug':   ('-d', '--debug',   True,             'Enable Debug', 'b'),
                    'logpath': ('-l', '--logpath', None,             'Path where log file is to be written'),
                    'redis':   ('-r', '--redis',   'localhost:6379', 'Redis connection string'),
                    'redisdb': ('',   '--redisdb', '8',              'Redis database'),
                  }

if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)

    log.info('Starting')

    log.info('Connecting to datastore')
    db = dbRedis(options)

    if db.ping():
        loadSAdata()
        indexPage(db)

    log.info('done')

