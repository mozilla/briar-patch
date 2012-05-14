#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" releng

    :copyright: (c) 2011 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Authors:
        catlee  Chris Atlee <catlee@mozilla.com>
        coop    Chris Cooper <coop@mozilla.com>
        bear    Mike Taylor <bear@mozilla.com>
"""

import os, sys
import re
import time
import json
import socket
import logging
import datetime
import ssh
import requests
import dns.resolver

from multiprocessing import get_logger
from . import fetchUrl, runCommand, relative, getPassword


log = get_logger()

urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'


class Host(object):
    def __init__(self, hostname, remoteEnv, verbose=False):
        self.verbose   = verbose
        self.remoteEnv = remoteEnv
        self.hostname  = hostname
        self.fqdn      = None
        self.ip        = None
        self.isTegra   = False
        self.hasPDU    = False
        self.hasIPMI   = False
        self.IPMIip    = None
        self.IPMIhost  = None
        self.channel   = None
        self.foopy     = None
        self.client    = None
        self.info      = None
        self.pinged    = False
        self.reachable = False

        logging.getLogger("ssh.transport").setLevel(logging.WARNING)

        if '.' in hostname:
            fullhostname = hostname
            hostname     = hostname.split('.', 1)[0]
        else:
            fullhostname = '%s.build.mozilla.org' % hostname

        if hostname in remoteEnv.hosts:
            self.info = remoteEnv.hosts[hostname]

        try:
            dnsAnswer = dns.resolver.query(fullhostname)
            self.fqdn = '%s' % dnsAnswer.canonical_name
            self.ip   = dnsAnswer[0]
        except:
            self.fqdn = None

        if self.fqdn is not None:
            try:
                self.IPMIhost = "%s-mgmt%s" % (hostname, self.fqdn.replace(hostname, ''))
                dnsAnswer     = dns.resolver.query(self.IPMIhost)
                self.IPMIip   = dnsAnswer[0]
                self.hasIPMI  = True
            except:
                self.IPMIhost = None
                self.IPMIip   = None

        print hostname
        print fullhostname
        print self.fqdn
        print self.ip

        if hostname.startswith('tegra'):
            self.isTegra = True

        if self.fqdn is not None:
            self.pinged, output = self.ping()
            if self.pinged:
                if verbose:
                    log.info('creating SSHClient')
                self.client = ssh.SSHClient()
                self.client.set_missing_host_key_policy(ssh.AutoAddPolicy())
            else:
                if verbose:
                    log.info('unable to ping %s' % hostname)

            if self.isTegra:
                self.bbdir = '/builds/%s' % hostname

                if hostname in remoteEnv.tegras:
                    self.foopy = remoteEnv.tegras[hostname]['foopy']

                try:
                    self.tegra = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                    self.tegra.settimeout(float(120))
                    self.tegra.connect((hostname, 20700))
                    self.reachable = True
                except:
                    log.error('socket error establishing connection to tegra data port', exc_info=True)
                    self.tegra = None

                if self.foopy is not None:
                    try:
                        self.client.connect('%s.build.mtv1.mozilla.com' % self.foopy, username=remoteEnv.sshuser, password=remoteEnv.sshPassword, allow_agent=False, look_for_keys=False)
                        self.transport = self.client.get_transport()
                        self.channel   = self.transport.open_session()
                        self.channel.get_pty()
                        self.channel.invoke_shell()
                    except:
                        log.error('socket error establishing ssh connection', exc_info=True)
                        self.client = None
            else:
                if self.pinged:
                    try:
                        if self.verbose:
                            log.info('connecting to remote host')
                        self.client.connect(self.fqdn, username=remoteEnv.sshuser, password=remoteEnv.sshPassword, allow_agent=False, look_for_keys=False)
                        self.transport = self.client.get_transport()
                        if self.verbose:
                            log.info('opening session')
                        self.channel   = self.transport.open_session()
                        self.channel.get_pty()
                        if self.verbose:
                            log.info('invoking remote shell')
                        self.channel.invoke_shell()
                        self.reachable = True
                    except:
                        log.error('socket error establishing ssh connection', exc_info=True)
                        self.client = None

    def graceful_shutdown(self, indent='', dryrun=False):
        if not self.buildbot_active():
            return False

        tacinfo = self.get_tacinfo()

        if tacinfo is None:
            log.error("%sCouldn't get info from buildbot.tac; host is disabled?" % indent)
            return False

        host, port, hostname = tacinfo

        if 'staging' in host:
            log.warn("%sIgnoring staging host %s for host %s" % (indent, host, self.hostname))
            return False

        # HTTP port is host port - 1000
        port -= 1000

        # Look at the host's page
        url = "http://%s:%i/buildslaves/%s" % (host, port, hostname)
        if self.verbose:
            log.info("%sFetching host page %s" % (indent, url))
        data = fetchUrl('%s?numbuilds=0' % url)

        if data is None:
            return False

        if "Graceful Shutdown" not in data:
            log.error("%sno shutdown form for %s" % (indent, self.hostname))
            return False

        if self.verbose:
            log.info("%sSetting shutdown" % indent)
        if dryrun:
            log.info("%sShutdown deferred" % indent)
        else:
            data = fetchUrl("%s/shutdown" % url)
            if data is None:
                return False

        return True

    def get_tacinfo(self):
        log.debug("Determining host's master")
        data   = self.cat_buildbot_tac()
        master = re.search('^buildmaster_host\s*=\s*["\'](.*)["\']', data, re.M)
        port   = re.search('^port\s*=\s*(\d+)', data, re.M)
        host   = re.search('^slavename\s*=\s*["\'](.*)["\']', data, re.M)
        if master and port and host:
            return master.group(1), int(port.group(1)), host.group(1)

    def run_cmd(self, cmd):
        log.debug("Running %s", cmd)
        if self.client is None:
            data = ''
        else:
            try:
                self.channel.sendall("%s\r\n" % cmd)
            except: # socket.error:
                log.error('socket error', exc_info=True)
                return
            data = self.wait()
        return data

    def ping(self):
        # bash-3.2$ ping -c 2 -o tegra-056
        # PING tegra-056.build.mtv1.mozilla.com (10.250.49.43): 56 data bytes
        # 64 bytes from 10.250.49.43: icmp_seq=0 ttl=64 time=1.119 ms
        #
        # --- tegra-056.build.mtv1.mozilla.com ping statistics ---
        # 1 packets transmitted, 1 packets received, 0.0% packet loss
        # round-trip min/avg/max/stddev = 1.119/1.119/1.119/0.000 ms
        out    = []
        result = False

        p, o = runCommand(['ping', '-c 5', self.fqdn], logEcho=False)
        for s in o:
            out.append(s)
            if '5 packets transmitted, 5 packets received' in s or '5 packets transmitted, 5 received' in s:
                result = True
                break
        return result, out

    def rebootPDU(self):
        """
        Try to reboot the given host, returning True if successful.

        snmpset -c private pdu4.build.mozilla.org 1.3.6.1.4.1.1718.3.2.3.1.11.1.1.13 i 3
        1.3.6.1.4.1.1718.3.2.3.1.11.a.b.c
                                    ^^^^^ outlet id
                                 ^^       control action
                               ^          outlet entry
                             ^            outlet tables
                           ^              system tables
                         ^                sentry
        ^^^^^^^^^^^^^^^^                  serverTech enterprises
        a   Sentry enclosure ID: 1 master 2 expansion
        b   Input Power Feed: 1 infeed-A 2 infeed-B
        c   Outlet ID (1 - 16)
        y   command: 1 turn on, 2 turn off, 3 reboot

        a and b are determined by the DeviceID we get from the tegras.json file

           .AB14
              ^^ Outlet ID
             ^   InFeed code
            ^    Enclosure ID (we are assuming 1 (or A) below)
        """
        result = False
        if self.hostname in self.remoteEnv.tegras:
            pdu      = self.remoteEnv.tegras[hostname]['pdu']
            deviceID = self.remoteEnv.tegras[hostname]['pduid']
            if deviceID.startswith('.'):
                if deviceID[2] == 'B':
                    b = 2
                else:
                    b = 1
                try:
                    log.debug('rebooting %s at %s %s' % (self.hostname, pdu, deviceID))
                    c   = int(deviceID[3:])
                    s   = '3.2.3.1.11.1.%d.%d' % (b, c)
                    oib = '1.3.6.1.4.1.1718.%s' % s
                    cmd = '/usr/bin/snmpset -c private %s %s i 3' % (pdu, oib)
                    if os.system(cmd) == 0:
                        result = True
                except:
                    log.error('error running [%s]' % cmd, exc_info=True)
                    result = False

        return result

        # code by Catlee, bugs by bear
    def rebootIPMI(self):
        result = False
        if self.hasIPMI:
            log.debug('logging into ipmi for %s at %s' % (self.hostname, self.IPMIip))
            try:
                r = requests.post("http://%s/cgi/login.cgi" % self.IPMIip,
                        data={ 'name': self.remoteEnv.ipmiUser,
                               'pwd':  self.remoteEnv.ipmiPassword,
                             })

                if r.status_code == 200:
                    # Push the button!
                    # e.g.
                    # http://10.12.48.105/cgi/ipmi.cgi?POWER_INFO.XML=(1%2C3)&time_stamp=Wed%20Mar%2021%202012%2010%3A26%3A57%20GMT-0400%20(EDT)
                    r = requests.get("http://%s/cgi/ipmi.cgi" % self.IPMIip,
                                     params={ 'POWER_INFO.XML': "(1,3)",
                                              'time_stamp': time.strftime("%a %b %d %Y %H:%M:%S"),
                                            },
                                     cookies = r.cookies
                                    )

                result = r.status_code == 200
            except:
                log.error('error connecting to IPMI', exc_info=True)
                result = False
        else:
            log.debug('IPMI not available')

        return result

class UnixishHost(Host):
    def _read(self):
        buf = []
        while self.channel.recv_ready():
            data = self.channel.recv(1024)
            if not data:
                break
            buf.append(data)
        buf = "".join(buf)

        # Strip out ANSI escape sequences
        # Setting position
        buf = re.sub('\x1b\[\d+;\d+f', '', buf)
        buf = re.sub('\x1b\[\d+m', '', buf)
        return buf

    def wait(self):
        log.debug('waiting for remote shell to respond')
        buf = []
        n   = 0
        if self.client is not None:
            while True:
                try:
                    self.channel.sendall("\r\n")
                    data = self._read()
                    buf.append(data)
                    if data.endswith(self.prompt) and not self.channel.recv_ready():
                        break
                    time.sleep(1)
                    n += 1
                    if n > 15:
                        log.error('timeout waiting for shell')
                        break
                except: # socket.error:
                    log.error('exception during wait()', exc_info=True)
                    self.client = None
                    break
        return "".join(buf)

    def buildbot_active(self):
        cmd  = 'ls -l %s/twistd.pid' % self.bbdir
        data = self.run_cmd(cmd)
        m    = re.search('No such file or directory$', data)
        if m:
            return False
        cmd  = 'ps ww `cat %s/twistd.pid`' % self.bbdir
        data = self.run_cmd(cmd)
        m    = re.search('buildbot', data)
        if m:
            return True
        return False

    def find_buildbot_tacfiles(self):
        cmd = "ls -l %s/buildbot.tac*" % self.bbdir
        data = self.run_cmd(cmd)
        tacs = []
        exp = "\d+ %s/(buildbot\.tac(?:\.\w+)?)" % self.bbdir
        for m in re.finditer(exp, data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "cat %s/buildbot.tac" % self.bbdir
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "tail -%i %s/twistd.log" % (n, self.bbdir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("sudo reboot")

class OSXTalosHost(UnixishHost):
    prompt = "cltbld$ "
    bbdir  = "/Users/cltbld/talos-slave"

class LinuxBuildHost(UnixishHost):
    prompt = "]$ "
    bbdir  = "/builds/slave"

class LinuxTalosHost(UnixishHost):
    prompt = "]$ "
    bbdir  = "/home/cltbld/talos-slave"

class OSXBuildHost(UnixishHost):
    prompt = "cltbld$ "
    bbdir  = "/builds/slave"

class WinHost(Host):
    def _read(self):
        buf = []
        if self.client is not None:
            while self.channel.recv_ready():
                data = self.channel.recv(1024)
                if not data:
                    break
                buf.append(data)
            buf = "".join(buf)

            # Strip out ANSI escape sequences
            # Setting position
            buf = re.sub('\x1b\[\d+;\d+f', '', buf)
        return buf

    def wait(self):
        buf = []
        n   = 0
        if self.client is not None:
            while True:
                try:
                    self.channel.sendall("\r\n")
                    data = self._read()
                    buf.append(data)
                    if data.endswith(">") and not self.channel.recv_ready():
                        break
                    time.sleep(1)
                    n += 1
                    if n > 15:
                        log.error('timeout waiting for shell')
                        break
                except: # socket.error:
                    log.error('socket error', exc_info=True)
                    self.client = None
                    break
        return "".join(buf)

    def buildbot_active(self):
        # for now just return True as it was assuming that it was active before
        return True

    def find_buildbot_tacfiles(self):
        cmd = "dir %s\\buildbot.tac*" % self.bbdir
        data = self.run_cmd(cmd)
        tacs = []
        for m in re.finditer("\d+ (buildbot\.tac(?:\.\w+)?)", data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "%scat.exe %s\\buildbot.tac" % (self.msysdir, self.bbdir)
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "%stail.exe -%i %s\\twistd.log" % (self.msysdir, n, self.bbdir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("shutdown -f -r -t 0")

class Win32BuildHost(WinHost):
    bbdir   = "E:\\builds\\moz2_slave"
    msysdir = 'D:\\mozilla-build\\msys\\bin\\'

class Win32TalosHost(WinHost):
    bbdir   = "C:\\talos-slave"
    msysdir = ''

class Win64BuildHost(WinHost):
    bbdir   = "E:\\builds\\moz2_slave"
    msysdir = ''

class Win64TalosHost(WinHost):
    bbdir   = "C:\\talos-slave"
    msysdir = ''

class TegraHost(UnixishHost):
    prompt = "cltbld$ "

    def reboot(self):
        self.rebootPDU()


def msg(msg, indent='', verbose=False):
    if verbose:
        log.info('%s%s' % (indent, msg))
    return msg

class RemoteEnvironment():
    def __init__(self, toolspath, sshuser='cltbld', ldapUser=None, ipmiUser='ADMIN'):
        self.toolspath = toolspath
        self.sshuser   = sshuser
        self.ldapUser  = ldapUser
        self.ipmiUser  = ipmiUser
        self.tegras    = {}
        self.hosts     = {}
        self.masters   = {}

        if self.sshuser is not None:
            self.sshPassword = getPassword(self.sshuser)

        if self.ldapUser is not None:
            self.ldapPassword = getPassword(self.ldapUser)

        if self.ipmiUser is not None:
            self.ipmiPassword = getPassword(self.ipmiUser)

        if not self.loadTegras(os.path.join(self.toolspath, 'buildfarm/mobile')):
            self.loadTegras('.')

        self.getHostInfo()

    def findMaster(self, masterName):
        for m in self.masters:
            master = self.masters[m]
            if master['nickname'] == masterName or masterName in master['fqdn']:
                return master
        return None

    def getHostInfo(self):
        self.hosts = {}
        # grab and process slavealloc list into a simple dictionary
        j = fetchUrl('%s/slaves' % urlSlaveAlloc)
        if j is None:
            hostlist = []
        else:
            hostlist = json.loads(j)

        self.masters = {}
        j = fetchUrl('%s/masters' % urlSlaveAlloc)
        if j is not None:
            m = json.loads(j)
            for item in m:
                self.masters[item['nickname']] = item

        environments = {}
        j = fetchUrl('%s/environments' % urlSlaveAlloc)
        if j is not None:
            e = json.loads(j)
            for item in e:
                environments[item['envid']] = item['name']

        for item in hostlist:
            if item['envid'] in environments:
                item['environment'] = environments[item['envid']]
            if item['notes'] is None:
                item['notes'] = ''
            self.hosts[item['name']] = item

    def getHost(self, hostname, verbose=False):
        result = None

        if 'w32-ix' in hostname or 'mw32-ix' in hostname or \
           'moz2-win32' in hostname or 'try-w32-' in hostname or \
           'win32-' in hostname:
            result = Win32BuildHost(hostname, self, verbose=verbose)

        elif 'w64-ix' in hostname:
            result = Win64BuildHost(hostname, self, verbose=verbose)

        elif 'talos-r3-fed' in hostname:
            result = LinuxTalosHost(hostname, self, verbose=verbose)

        elif 'talos-r3-snow' in hostname or 'talos-r4' in hostname or \
             'talos-r3-leopard' in hostname:
            result = OSXTalosHost(hostname, self, verbose=verbose)

        elif 'talos-r3-xp' in hostname or 'w764' in hostname or \
             'talos-r3-w7' in hostname:
            result = Win32TalosHost(hostname, self, verbose=verbose)

        elif 'moz2-linux' in hostname or 'linux-ix' in hostname or \
             'try-linux' in hostname or 'linux64-ix-' in hostname or \
             'bld-centos6' in hostname:
            result = LinuxBuildHost(hostname, self, verbose=verbose)

        elif 'try-mac' in hostname or 'xserve' in hostname or \
             'moz2-darwin' in hostname or 'bld-lion-r5' in hostname:
            result = OSXBuildHost(hostname, self, verbose=verbose)

        elif 'tegra' in hostname:
            result = TegraHost(hostname, self, verbose=verbose)
        else:
            log.error("Unknown host type for %s", hostname)
            result = None

        if result is not None:
            result.wait()

        return result

    def loadTegras(self, toolspath):
        result = False
        tFile  = os.path.join(toolspath, 'tegras.json')

        if os.path.isfile(tFile):
            try:
                self.tegras = json.load(open(tFile, 'r'))
                result = True
            except:
                log.error('error loading tegras.json from %s' % tFile, exc_info=True)

        return result

    def rebootIfNeeded(self, host, lastSeen=None, indent='', dryrun=True, verbose=False):
        reboot    = False
        recovery  = False
        reachable = False
        ipmi      = False
        pdu       = False
        failed    = False
        output    = []

        if host is not None:
            reachable = host.reachable

        if not reachable:
            output.append(msg('adding to recovery list because host is not reachable', indent, verbose))
            recovery = True
        if lastSeen is None:
            output.append(msg('adding to recovery list because last activity is unknown', indent, verbose))
            recovery = True
        else:
            hours  = (lastSeen.days * 24) + (lastSeen.seconds / 3600)
            reboot = hours > 6
            output.append(msg('last activity %0.2d hours' % hours, indent, verbose))

        # if we can ssh to host, then try and do normal shutdowns
        if reachable and reboot:
            if host.graceful_shutdown(indent=indent, dryrun=dryrun):
                if not dryrun:
                    if verbose:
                        log.info("%sWaiting for shutdown" % indent)
                    count = 0

                    while True:
                        count += 1
                        if count >= 30:
                            failed = True
                            if verbose:
                                log.info("%sTook too long to shut down; giving up" % indent)
                            break

                        data = host.tail_twistd_log(10)
                        if not data or "Main loop terminated" in data or "ProcessExitedAlready" in data:
                            break
            else:
                failed = True
                if verbose:
                    log.info("%sgraceful_shutdown failed" % indent)

        if host is not None:
            if recovery:
                ipmi = host.hasIPMI
                pdu  = host.hasPDU

                if lastSeen is not None:
                    if host.isTegra:
                        if not dryrun:
                            pdu = host.rebootPDU()
                        reboot = True
                    else:
                        if host.hasIPMI:
                            if not dryrun:
                                ipmi = host.rebootIPMI()
                            reboot = True
                        else:
                            output.append(msg('should be restarting but not reachable and no IPMI', indent, True))
            else:
                if reboot and not dryrun:
                    host.reboot()

        if failed:
            reboot   = False
            recovery = True
            if verbose:
                log.info('reboot failed, forcing recovery flag')

        return { 'reboot': reboot, 'recovery': recovery, 'output': output, 'ipmi': ipmi, 'pdu': pdu, 'dryrun': dryrun }

    def check(self, host, indent='', dryrun=True, verbose=False, reboot=False):
        status = { 'buildbot':  '',
                   'tacfile':   '',
                   'master':    '',
                   'fqdn':      '',
                   'reachable': False,
                   'lastseen':  None,
                   'output':    [],
                 }

        if host and host.fqdn:
            status['fqdn'] = host.fqdn

        if host is not None and host.reachable:
            status['reachable'] = host.reachable

            host.wait()

            tacfiles = host.find_buildbot_tacfiles()
            if "buildbot.tac" in tacfiles:
                status['tacfile'] = 'found'
                status['master']  = host.get_tacinfo()
            else:
                status['tacfile'] = 'NOT FOUND'
                if verbose:
                    log.info("%sbuildbot.tac NOT FOUND" % indent)

            if verbose:
                log.info("%sFound these tacfiles: %s" % (indent, tacfiles))
            for tac in tacfiles:
                m = re.match("^buildbot.tac.bug(\d+)$", tac)
                if m:
                    if verbose:
                        log.info("%sDisabled by bug %s" % (indent, m.group(1)))
                    status['tacfile'] = 'bug %s' % m.group(1)
                    break

            if host.buildbot_active():
                status['buildbot'] = status['buildbot'] + '; running'
            else:
                status['buildbot'] = status['buildbot'] + '; NOT running'

            data = host.tail_twistd_log(200)
            if len(data) > 0:
                lines = data.split('\n')
                logTD = None
                logTS = None
                for line in reversed(lines):
                    if '[Broker,client]' in line:
                        try:
                            logTS = datetime.datetime.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                            logTD = datetime.datetime.now() - logTS
                        except:
                            log.info('unable to parse the log date', exc_info=True)
                            logTD = None
                        if verbose:
                            log.debug('%stail: %s' % (indent, line))
                        break
                if logTD is not None:
                    status['lastseen'] = logTD
                    if (logTD.days == 0) and (logTD.seconds <= 3600):
                        status['buildbot'] = status['buildbot'] + '; active'

            data = host.tail_twistd_log(10)
            if "Stopping factory" in data:
                status['buildbot'] = status['buildbot'] + '; factory stopped'
                if verbose:
                    log.info("%sLooks like the host isn't connected" % indent)
        else:
            log.error('%sUnable to control host remotely' % indent)

        if len(status['buildbot']) > 0:
            status['buildbot'] = status['buildbot'][2:]

        if status['reachable']:
            s = ''
            if status['tacfile'] != 'found':
                s += '; tacfile: %s' % status['tacfile']
            if len(status['buildbot']) > 0:
                s += '; buildbot: %s' % status['buildbot']
            if s.startswith('; '):
                s = s[2:]
        else:
            s = 'OFFLINE'

        if len(s) > 0:
            log.info('%s%s' % (indent, s))

        if reboot:
            d = self.rebootIfNeeded(host, lastSeen=status['lastseen'], indent=indent, dryrun=dryrun, verbose=verbose)
            for s in ['reboot', 'recovery', 'ipmi', 'pdu']:
                status[s] = d[s]
            status['output'] += d['output']

        return status

