#!/usr/bin/env python

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

from multiprocessing import get_logger
from . import fetchUrl, runCommand, relative


log = get_logger()

urlSlaveAlloc = 'http://slavealloc.build.mozilla.org/api'


class Slave(object):
    def __init__(self, remoteEnv, hostname, verbose=False):
        self.verbose   = verbose
        self.remoteEnv = remoteEnv
        self.hostname  = hostname
        self.isTegra   = False
        self.client    = paramiko.SSHClient()
        self.channel   = None
        self.foopy     = None
        self.reachable = False

        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if hostname.startswith('tegra'):
            self.isTegra = True

        if self.isTegra:
            self.slavedir = '/builds/%s' % hostname

            if hostname in remoteEnv.tegras:
                self.foopy = remoteEnv.tegras[hostname]['foopy']

            try:
                self.tegra = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                self.tegra.settimeout(float(120))
                self.tegra.connect((hostname, 20700))
                self.reachable = True
            except:
                log.error('socket error establishing connection to tegra data port')
                self.tegra = None

            if self.foopy is not None:
                try:
                    self.client.connect('%s.build.mtv1.mozilla.com' % self.foopy, username=remoteEnv.username, password=remoteEnv.password, allow_agent=False, look_for_keys=False)
                    self.transport = self.client.get_transport()
                    self.channel   = self.transport.open_session()
                    self.channel.get_pty()
                    self.channel.invoke_shell()
                except:
                    log.error('socket error establishing ssh connection')
                    self.client = None
        else:
            try:
                self.client.connect(hostname, username=remoteEnv.username, password=remoteEnv.password, allow_agent=False, look_for_keys=False)
                self.transport = self.client.get_transport()
                self.channel   = self.transport.open_session()
                self.channel.get_pty()
                self.channel.invoke_shell()
                self.reachable = True
            except:
                log.error('socket error establishing ssh connection')
                self.client = None

    def graceful_shutdown(self, indent='', dryrun=False):
        if not self.buildbot_active():
            return False

        tacinfo = self.get_tacinfo()

        if tacinfo is None:
            log.error("%sCouldn't get info from buildbot.tac; slave is disabled?" % indent)
            return False

        host, port, slavename = tacinfo

        if 'staging' in host:
            log.warn("%sIgnoring staging host %s for slave %s" % (indent, host, self.hostname))
            return False

        # HTTP port is slave port - 1000
        port -= 1000

        # Look at the slave's page
        url = "http://%s:%i/buildslaves/%s" % (host, port, slavename)
        if self.verbose:
            log.info("%sFetching slave page %s" % (indent, url))
        data = fetchUrl('%s?numbuilds=0' % url)

        #if "not currently connected" in data:
            #log.error("%s isn't connected!", self.hostname)
            # reboot now?
            #return False

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
        log.debug("Determining slave's master")
        data = self.cat_buildbot_tac()
        host = re.search('^buildmaster_host\s*=\s*["\'](.*)["\']', data, re.M)
        port = re.search('^port\s*=\s*(\d+)', data, re.M)
        slave = re.search('^slavename\s*=\s*["\'](.*)["\']', data, re.M)
        if host and port and slave:
            return host.group(1), int(port.group(1)), slave.group(1)

    def run_cmd(self, cmd):
        log.debug("Running %s", cmd)
        if self.client is None:
            data = ''
        else:
            try:
                self.channel.sendall("%s\r\n" % cmd)
            except socket.error:
                log.error('socket error')
                return
            data = self.wait()
        return data

class UnixishSlave(Slave):
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
        buf = []
        if self.client is not None:
            while True:
                try: 
                    self.channel.sendall("\r\n")
                    data = self._read()
                    buf.append(data)
                    if data.endswith(self.prompt) and not self.channel.recv_ready():
                        break
                    time.sleep(1)
                except socket.error:
                    log.error('socket error')
                    self.client = None
                    break
        return "".join(buf)

    def buildbot_active(self):
        cmd  = 'ls -l %s/twistd.pid' % self.slavedir
        data = self.run_cmd(cmd)
        m    = re.search('No such file or directory$', data)
        if m:
            return False
        cmd  = 'ps `cat %s/twistd.pid`' % self.slavedir
        data = self.run_cmd(cmd)
        m    = re.search('buildbot', data)
        if m:
            return True
        return False

    def find_buildbot_tacfiles(self):
        cmd = "ls -l %s/buildbot.tac*" % self.slavedir
        data = self.run_cmd(cmd)
        tacs = []
        exp = "\d+ %s/(buildbot\.tac(?:\.\w+)?)" % self.slavedir
        for m in re.finditer(exp, data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "cat %s/buildbot.tac" % self.slavedir
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "tail -%i %s/twistd.log" % (n, self.slavedir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("sudo reboot")

class OSXTalosSlave(UnixishSlave):
    prompt = "cltbld$ "
    slavedir = "/Users/cltbld/talos-slave"

class LinuxBuildSlave(UnixishSlave):
    prompt = "]$ "
    slavedir = "/builds/slave"

class LinuxTalosSlave(UnixishSlave):
    prompt = "]$ "
    slavedir = "/home/cltbld/talos-slave"

class OSXBuildSlave(UnixishSlave):
    prompt = "cltbld$ "
    slavedir = "/builds/slave"

class WinSlave(Slave):
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
        if self.client is not None:
            while True:
                try: 
                    self.channel.sendall("\r\n")
                    data = self._read()
                    buf.append(data)
                    if data.endswith(">") and not self.channel.recv_ready():
                        break
                    time.sleep(1)
                except socket.error:
                    log.error('socket error')
                    self.client = None
                    break
        return "".join(buf)

    def buildbot_active(self):
        # for now just return True as it was assuming that it was active before
        return True

    def find_buildbot_tacfiles(self):
        cmd = "dir %s\\buildbot.tac*" % self.slavedir
        data = self.run_cmd(cmd)
        tacs = []
        for m in re.finditer("\d+ (buildbot\.tac(?:\.\w+)?)", data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "%scat.exe %s\\buildbot.tac" % (self.msysdir, self.slavedir)
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "%stail.exe -%i %s\\twistd.log" % (self.msysdir, n, self.slavedir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("shutdown -f -r -t 0")

class Win32BuildSlave(WinSlave):
    slavedir = "E:\\builds\\moz2_slave"
    msysdir  = 'D:\\mozilla-build\\msys\\bin\\'

class Win32TalosSlave(WinSlave):
    slavedir = "C:\\talos-slave"
    msysdir  = ''

class Win64BuildSlave(WinSlave):
    slavedir = "E:\\builds\\moz2_slave"
    msysdir  = ''

class Win64TalosSlave(WinSlave):
    slavedir = "C:\\talos-slave"
    msysdir  = ''

class TegraSlave(Slave):
    prompt = "cltbld$ "

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
            buf = re.sub('\x1b\[\d+m', '', buf)
        return buf

    def wait(self):
        buf = []
        if self.client is not None:
            while True:
                try: 
                    self.channel.sendall("\r\n")
                except socket.error:
                    log.error('socket error')
                    break
                data = self._read()
                buf.append(data)
                if data.endswith(self.prompt) and not self.channel.recv_ready():
                    break
                time.sleep(1)
        return "".join(buf)

    def find_buildbot_tacfiles(self):
        cmd = "ls -l /builds/%s/buildbot.tac*" % self.hostname
        data = self.run_cmd(cmd)
        tacs = []
        exp = "\d+ %s/(buildbot\.tac(?:\.\w+)?)" % self.slavedir
        for m in re.finditer(exp, data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "cat %s/buildbot.tac" % self.slavedir
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "tail -%i %s/twistd.log" % (n, self.slavedir)
        return self.run_cmd(cmd)

    def graceful_shutdown(self, indent='', dryrun=False):
        return False

    def get_tacinfo(self):
        return False

    def reboot(self):
        self.remoteEnv.rebootPDU(self.hostname, debug=True)


class RemoteEnvironment():
    def __init__(self, toolspath, username, password):
        self.toolspath = toolspath
        self.username  = username
        self.password  = password
        self.tegras    = {}
        self.slaves    = {}
        self.slave     = None

        if not self.loadTegras(os.path.join(self.toolspath, 'buildfarm/mobile')):
            self.loadTegras('.')

        # grab and process slavealloc list into a simple dictionary
        j = fetchUrl('%s/slaves' % urlSlaveAlloc)
        if j is None:
            slavelist = []
        else:
            slavelist = json.loads(j)

        environments = {}
        j = fetchUrl('%s/environments' % urlSlaveAlloc)
        if j is not None:
            environ = json.loads(j)
            for item in environ:
                environments[item['envid']] = item['name']

        for item in slavelist:
            if item['envid'] in environments:
                item['environment'] = environments[item['envid']]
            if item['notes'] is None:
                item['notes'] = ''
            self.slaves[item['name']] = item

    def getSlave(self, hostname, verbose=False):
        if self.slave is not None and self.slave.hostname != hostname:
            self.slave = None

        if self.slave is None:
            if 'w32-ix' in hostname or 'mw32-ix' in hostname or \
               'moz2-win32' in hostname or 'try-w32-' in hostname or \
               'win32-' in hostname:
                self.slave = Win32BuildSlave(self, hostname, verbose=verbose)

            if 'w64-ix' in hostname:
                self.slave = Win64BuildSlave(self, hostname, verbose=verbose)

            elif 'talos-r3-fed' in hostname:
                self.slave = LinuxTalosSlave(self, hostname, verbose=verbose)

            elif 'talos-r3-snow' in hostname or 'talos-r4' in hostname:
                self.slave = OSXTalosSlave(self, hostname, verbose=verbose)

            elif 'talos-r3-xp' in hostname or 'w764' in hostname:
                self.slave = Win32TalosSlave(self, hostname, verbose=verbose)

            elif 'moz2-linux' in hostname or 'linux-ix' in hostname or \
                 'try-linux' in hostname or 'linux64-ix-' in hostname:
                self.slave = LinuxBuildSlave(self, hostname, verbose=verbose)

            elif 'try-mac' in hostname or 'xserve' in hostname or \
                 'moz2-darwin' in hostname:
                self.slave = OSXBuildSlave(self, hostname, verbose=verbose)

            elif 'tegra' in hostname:
                self.slave = TegraSlave(self, hostname, verbose=verbose)
            else:
                log.error("Unknown slave type for %s", hostname)
                self.slave = None

        return self.slave

    def loadTegras(self, toolspath):
        result = False
        tFile  = os.path.join(toolspath, 'tegras.json')

        if os.path.isfile(tFile):
            try:
                self.tegras = json.load(open(tFile, 'r'))
                result = True
            except:
                log.error('error loading tegras.json from %s' % tFile)

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
        p, o = runCommand(['ping', '-c 5', '-o', hostname], logEcho=False)
        for s in o:
            out.append(s)
            if '1 packets transmitted, 1 packets received' in s:
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
                    log.error('error running [%s]' % cmd)
                    result = False

        return result

    def rebootIfNeeded(self, hostname, lastSeen=None, indent='', dryrun=True, verbose=False):
        reboot    = False
        recovery  = False
        reachable = False

        self.getSlave(hostname, verbose=verbose)

        if self.slave is not None:
            reachable = self.slave.reachable

        if not reachable:
            if verbose:
                log.info('%sadding to recovery list because host is not reachable' % indent)
            recovery = True
        if lastSeen is None:
            if verbose:
                log.info('%adding to recovery list because last activity is unknown' % indent)
            recovery = True
        else:
            hours  = (lastSeen.days * 24) + (lastSeen.seconds / 3600)
            reboot = hours > 6
            log.info('%slast activity %0.2d hours' % (indent, hours))

        # if we can ssh to host, then try and do normal shutdowns
        if reachable and reboot:
            if self.slave.graceful_shutdown(indent=indent, dryrun=dryrun):
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

                        data = self.slave.tail_twistd_log(10)
                        if not data or "Main loop terminated" in data or "ProcessExitedAlready" in data:
                            break
            else:
                if verbose:
                    log.info("%sgraceful_shutdown failed" % indent)

        if dryrun and reboot:
            log.info('%sREBOOT deferred' % indent)
            reboot = False

        if dryrun and recovery:
            log.info('%sRECOVERY deferred' % indent)
            recovery = False

        if recovery:
            log.info('%sRECOVERY (todo)' % indent)
            #TODO
        else:
            if reboot:
                if reachable:
                    log.info('%sREBOOT' % indent)
                    self.slave.reboot()
                else:
                    log.info('%sshould be REBOOTing but not reachable and no PDU' % indent)

    def check(self, hostname, indent='', dryrun=True, verbose=False, reboot=False):
        self.getSlave(hostname, verbose=verbose)

        status = { 'buildbot':  '',
                   'tacfile':   '',
                   'reachable': False,
                   'lastseen':  None,
                 }

        if self.slave is not None and self.slave.reachable:
            status['reachable'] = self.slave.reachable

            self.slave.wait()

            tacfiles = self.slave.find_buildbot_tacfiles()
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

            data = self.slave.tail_twistd_log(200)
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
                        status['buildbot'] = 'active'

            data = self.slave.tail_twistd_log(10)
            if "Stopping factory" in data:
                status['buildbot'] = 'factory stopped'
                if verbose:
                    log.info("%sLooks like the slave isn't connected" % indent)
        else:
            log.error('%sUnable to control host remotely' % indent)

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
            self.rebootIfNeeded(hostname, lastSeen=status['lastseen'], indent=indent, dryrun=dryrun, verbose=verbose)

        return status

