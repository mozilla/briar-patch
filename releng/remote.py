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
import paramiko
import requests

from multiprocessing import get_logger
from . import fetchUrl, runCommand, relative, getPassword


log = get_logger()

urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'


class Host(object):
    def __init__(self, remoteEnv, hostname, verbose=False):
        self.verbose   = verbose
        self.remoteEnv = remoteEnv
        self.hostname  = hostname
        self.isTegra   = False
        self.channel   = None
        self.foopy     = None
        self.client    = None
        self.pinged    = False
        self.reachable = False

        logging.getLogger("paramiko.transport").setLevel(logging.WARNING)

        if hostname.startswith('tegra'):
            self.isTegra = True

        self.pinged, output = self.remoteEnv.ping(hostname)
        if self.pinged:
            if verbose:
                log.info('creating SSHClient')
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
                    self.client.connect(remoteEnv.hosts[hostname]['fqdn'], username=remoteEnv.sshuser, password=remoteEnv.sshPassword, allow_agent=False, look_for_keys=False)
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
        self.remoteEnv.rebootPDU(self.hostname, debug=True)


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
        self.host      = None

        if self.sshuser is not None:
            self.sshPassword = getPassword(self.sshuser)

        if self.ldapUser is not None:
            self.ldapPassword = getPassword(self.ldapUser)

        if self.ipmiUser is not None:
            self.ipmiPassword = getPassword(self.ipmiUser)

        if not self.loadTegras(os.path.join(self.toolspath, 'buildfarm/mobile')):
            self.loadTegras('.')

        self.getHostInfo()

    def getHostInfo(self):
        self.hosts = {}
        # grab and process slavealloc list into a simple dictionary
        j = fetchUrl('%s/slaves' % urlSlaveAlloc)
        if j is None:
            hostlist = []
        else:
            hostlist = json.loads(j)

        environments = {}
        j = fetchUrl('%s/environments' % urlSlaveAlloc)
        if j is not None:
            environ = json.loads(j)
            for item in environ:
                environments[item['envid']] = item['name']

        for item in hostlist:
            if item['envid'] in environments:
                item['environment'] = environments[item['envid']]
            if item['notes'] is None:
                item['notes'] = ''
            item['fqdn'] = '%s.build.%s.mozilla.com' % (item['name'], item['datacenter'])
            self.hosts[item['name']] = item

    def getHost(self, hostname, verbose=False):
        if self.host is not None and self.host.hostname != hostname:
            self.host = None

        if self.host is None:
            if 'w32-ix' in hostname or 'mw32-ix' in hostname or \
               'moz2-win32' in hostname or 'try-w32-' in hostname or \
               'win32-' in hostname:
                self.host = Win32BuildHost(self, hostname, verbose=verbose)

            elif 'w64-ix' in hostname:
                self.host = Win64BuildHost(self, hostname, verbose=verbose)

            elif 'talos-r3-fed' in hostname:
                self.host = LinuxTalosHost(self, hostname, verbose=verbose)

            elif 'talos-r3-snow' in hostname or 'talos-r4' in hostname or 'talos-r3-leopard' in hostname:
                self.host = OSXTalosHost(self, hostname, verbose=verbose)

            elif 'talos-r3-xp' in hostname or 'w764' in hostname or 'talos-r3-w7' in hostname:
                self.host = Win32TalosHost(self, hostname, verbose=verbose)

            elif 'moz2-linux' in hostname or 'linux-ix' in hostname or \
                 'try-linux' in hostname or 'linux64-ix-' in hostname:
                self.host = LinuxBuildHost(self, hostname, verbose=verbose)

            elif 'try-mac' in hostname or 'xserve' in hostname or \
                 'moz2-darwin' in hostname:
                self.host = OSXBuildHost(self, hostname, verbose=verbose)

            elif 'tegra' in hostname:
                self.host = TegraHost(self, hostname, verbose=verbose)
            else:
                log.error("Unknown host type for %s", hostname)
                self.host = None

        return self.host

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

    def ping(self, hostname):
        # bash-3.2$ ping -c 2 -o tegra-056
        # PING tegra-056.build.mtv1.mozilla.com (10.250.49.43): 56 data bytes
        # 64 bytes from 10.250.49.43: icmp_seq=0 ttl=64 time=1.119 ms
        # 
        # --- tegra-056.build.mtv1.mozilla.com ping statistics ---
        # 1 packets transmitted, 1 packets received, 0.0% packet loss
        # round-trip min/avg/max/stddev = 1.119/1.119/1.119/0.000 ms
        out    = []
        result = False
        p, o = runCommand(['ping', '-c 5', self.hosts[hostname]['fqdn']], logEcho=False)
        for s in o:
            out.append(s)
            if '5 packets transmitted, 5 packets received' in s or '5 packets transmitted, 5 received' in s:
                result = True
                break
        return result, out

    def rebootPDU(self, hostname, debug=False):
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
        if hostname in self.tegras:
            pdu      = self.tegras[hostname]['pdu']
            deviceID = self.tegras[hostname]['pduid']
            if deviceID.startswith('.'):
                if deviceID[2] == 'B':
                    b = 2
                else:
                    b = 1
                try:
                    log.debug('rebooting %s at %s %s' % (hostname, pdu, deviceID))
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
    def rebootIPMI(self, hostname, debug=False):
        try:
            ip = socket.gethostbyname("%s-mgmt.build.mozilla.org" % hostname)
        except:
            ip = None

        log.debug('logging into ipmi for %s at %s' % (hostname, ip))
        r = requests.post("http://%s/cgi/login.cgi" % ip,
                data={ 'name': self.ipmiUser,
                       'pwd':  self.ipmiPassword,
                     })

        if r.status_code == 200:
            # Push the button!
            # e.g.
            # http://10.12.48.105/cgi/ipmi.cgi?POWER_INFO.XML=(1%2C3)&time_stamp=Wed%20Mar%2021%202012%2010%3A26%3A57%20GMT-0400%20(EDT)
            r = requests.get("http://%s/cgi/ipmi.cgi" % ip,
                             params={ 'POWER_INFO.XML': "(1,3)",
                                      'time_stamp': time.strftime("%a %b %d %Y %H:%M:%S"),
                                    },
                             cookies = r.cookies
                            )

        return r.status_code == 200


    def rebootIfNeeded(self, hostname, lastSeen=None, indent='', dryrun=True, verbose=False):
        reboot    = False
        recovery  = False
        reachable = False
        output    = []

        self.getHost(hostname, verbose=verbose)

        if self.host is not None:
            reachable = self.host.reachable

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
            if self.host.graceful_shutdown(indent=indent, dryrun=dryrun):
                if not dryrun:
                    if verbose:
                        log.info("%sWaiting for shutdown" % indent)
                    count = 0

                    while True:
                        count += 1
                        if count >= 30:
                            if verbose:
                                log.info("%sTook too long to shut down; giving up" % indent)
                            break

                        data = self.host.tail_twistd_log(10)
                        if not data or "Main loop terminated" in data or "ProcessExitedAlready" in data:
                            break
            else:
                if verbose:
                    log.info("%sgraceful_shutdown failed" % indent)

        if dryrun and reboot:
            output.append(msg('REBOOT deferred', indent, True))
            reboot = False

        if dryrun and recovery:
            output.append(msg('RECOVERY deferred', indent, True))
            recovery = False

        if self.host is not None:
            if recovery:
                if self.host.isTegra:
                    output.append(msg('RECOVERY-PDU', indent, True))
                    self.rebootPDU(hostname)
                    reboot = True
                else:
                    try:
                        # FIXME 
                        # yes, we are depending on this call to FAIL to let us know
                        # if the host is manageable by IPMI ... YUCK
                        ip = socket.gethostbyname("%s-mgmt.build.mozilla.org" % hostname)
                        self.rebootIPMI(hostname)
                        reboot = True
                        output.append(msg('RECOVERY-IPMI', indent, True))
                    except:
                        output.append(msg('should be restarting but not reachable and no IPMI', indent, True))
            else:
                if reboot:
                    self.host.reboot()
                    output.append(msg('REBOOT', indent, True))

        return { 'reboot': reboot, 'recovery': recovery, 'output': output }

    def check(self, hostname, indent='', dryrun=True, verbose=False, reboot=False):
        self.getHost(hostname, verbose=verbose)

        status = { 'buildbot':  '',
                   'tacfile':   '',
                   'reachable': False,
                   'lastseen':  None,
                   'output':    [],
                 }

        if self.host is not None and self.host.reachable:
            status['reachable'] = self.host.reachable

            self.host.wait()

            tacfiles = self.host.find_buildbot_tacfiles()
            if "buildbot.tac" in tacfiles:
                status['tacfile'] = 'found'
            else:
                if verbose:
                    log.info("%sFound these tacfiles: %s" % (indent, tacfiles))
                status['tacfile'] = 'NOT FOUND'
                for tac in tacfiles:
                    m = re.match("^buildbot.tac.bug(\d+)$", tac)
                    if m:
                        if verbose:
                            log.info("%sDisabled by bug %s" % (indent, m.group(1)))
                        status['tacfile'] = 'bug %s' % m.group(1)
                        break
                if status['tacfile'] == 'NOT FOUND':
                    if verbose:
                        log.info("%sbuildbot.tac NOT FOUND" % indent)

            if self.host.buildbot_active():
                status['buildbot'] = status['buildbot'] + '; running'
            else:
                status['buildbot'] = status['buildbot'] + '; NOT running'

            data = self.host.tail_twistd_log(200)
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

            data = self.host.tail_twistd_log(10)
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
            d = self.rebootIfNeeded(hostname, lastSeen=status['lastseen'], indent=indent, dryrun=dryrun, verbose=verbose)
            for s in ['reboot', 'recovery']:
                status[s] = d[s]
            status['output'] += d['output']

        return status

