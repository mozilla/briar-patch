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
import paramiko

from multiprocessing import get_logger
from . import fetchUrl, runCommand


log = get_logger()


class Slave(object):
    def __init__(self, remoteEnv, hostname):
        self.remoteEnv = remoteEnv
        self.hostname  = hostname
        self.isTegra   = False
        self.client    = paramiko.SSHClient()
        self.channel   = None
        self.tegra     = None
        self.foopy     = None

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
            except:
                log.error('socket error establishing ssh connection')
                self.client = None

    def graceful_shutdown(self):
        tacinfo = self.get_tacinfo()
        if tacinfo is None:
            log.error("Couldn't get info from buildbot.tac; slave is disabled?")
            return False

        host, port, slavename = tacinfo

        if 'staging' in host:
            log.warn("Ignoring staging host %s for slave %s", host, self.hostname)
            return False

        # HTTP port is slave port - 1000
        port -= 1000

        # Look at the slave's page
        url = "http://%s:%i/buildslaves/%s" % (host, port, slavename)
        log.info("Fetching slave page %s", url)
        data = fetchUrl('%s?numbuilds=0' % url)

        #if "not currently connected" in data:
            #log.error("%s isn't connected!", self.hostname)
            # reboot now?
            #return False

        if "Graceful Shutdown" not in data:
            log.error("no shutdown form for %s", self.hostname)
            return False

        log.info("Setting shutdown")
        data = fetchUrl("%s/shutdown" % url)
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

class Win32Slave(Slave):
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

class Win32BuildSlave(Win32Slave):
    slavedir = "E:\\builds\\moz2_slave"

    def find_buildbot_tacfiles(self):
        cmd = "dir %s\\buildbot.tac*" % self.slavedir
        data = self.run_cmd(cmd)
        tacs = []
        for m in re.finditer("\d+ (buildbot\.tac(?:\.\w+)?)", data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "D:\\mozilla-build\\msys\\bin\\cat.exe %s\\buildbot.tac" % self.slavedir
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "D:\\mozilla-build\\msys\\bin\\tail.exe -%i %s\\twistd.log" % (n, self.slavedir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("shutdown -f -r -t 0")

class Win32TalosSlave(Win32Slave):
    slavedir = "C:\\talos-slave"

    def find_buildbot_tacfiles(self):
        cmd = "dir %s\\buildbot.tac*" % self.slavedir
        data = self.run_cmd(cmd)
        tacs = []
        for m in re.finditer("\d+ (buildbot\.tac(?:\.\w+)?)", data):
            tacs.append(m.group(1))
        return tacs

    def cat_buildbot_tac(self):
        cmd = "cat %s\\buildbot.tac" % self.slavedir
        return self.run_cmd(cmd)

    def tail_twistd_log(self, n=100):
        cmd = "tail -%i %s\\twistd.log" % (n, self.slavedir)
        return self.run_cmd(cmd)

    def reboot(self):
        self.run_cmd("shutdown -f -r -t 0")

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

    def graceful_shutdown(self):
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

        if not self.loadTegras(os.path.join(self.toolspath, 'buildfarm/mobile')):
            self.loadTegras('.')

    def getSlave(self, hostname):
        result = None
        if 'w32-ix' in hostname or 'moz2-win32' in hostname or \
           'try-w32-' in hostname or 'win32-' in hostname or \
           'w64-ix' in hostname:
            result = Win32BuildSlave(self, hostname)

        elif 'talos-r3-fed' in hostname:
            result = LinuxTalosSlave(self, hostname)

        elif 'talos-r3-snow' in hostname or 'talos-r4' in hostname:
            result = OSXTalosSlave(self, hostname)

        elif 'talos-r3-xp' in hostname or 'w764' in hostname:
            result = Win32TalosSlave(self, hostname)

        elif 'moz2-linux' in hostname or 'linux-ix' in hostname or \
             'try-linux' in hostname or 'linux64-ix-' in hostname:
            result = LinuxBuildSlave(self, hostname)

        elif 'try-mac' in hostname or 'xserve' in hostname:
            result = OSXBuildSlave(self, hostname)

        elif 'tegra' in hostname:
            result = TegraSlave(self, hostname)
        else:
            log.error("Unknown slave type for %s", hostname)
            result = None

        return result

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
        p, o = runCommand(['/sbin/ping', '-c 5', '-o', hostname], logEcho=False)
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

    def checkAndReboot(self, hostname, dryrun=True):
        slave = self.getSlave(hostname)

        if slave is not None:
            slave.wait()

            tacfiles = slave.find_buildbot_tacfiles()
            if "buildbot.tac" not in tacfiles:
                log.info("Found these tacfiles: %s", tacfiles)
                for tac in tacfiles:
                    m = re.match("^buildbot.tac.bug(\d+)$", tac)
                    if m:
                        log.info("Disabled by bug %s" % m.group(1))
                        return
                log.info("Didn't find buildbot.tac")
                return

            data = slave.tail_twistd_log(10)
            if "Stopping factory" in data:
                log.info("Looks like the slave isn't connected; rebooting!")
                if dryrun:
                    log.info('reboot deferred')
                else:
                    slave.reboot()
                return

            if slave.isTegra:
                if dryrun:
                    log.info('reboot deferred')
                else:
                    slave.reboot()
            else:
                if dryrun:
                    log.info('shutdown deferred')
                else:
                    if not slave.graceful_shutdown():
                        log.info("graceful_shutdown failed; aborting")
                        return
                    log.info("Waiting for shutdown")
                    count = 0

                    while True:
                        count += 1
                        if count >= 30:
                            log.info("Took too long to shut down; giving up")
                            data = slave.tail_twistd_log(10)
                            if data:
                                log.info("last 10 lines are: %s", data)
                            break

                        data = slave.tail_twistd_log(5)
                        if not data or "Main loop terminated" in data or "ProcessExitedAlready" in data:
                            log.info("Rebooting!")
                            if dryrun:
                                log.info('reboot deferred')
                            else:
                                slave.reboot()
                            break
