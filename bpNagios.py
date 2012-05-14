#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

""" bpNagios

    :copyright: (c) 2012 by Mozilla
    :license: MPLv2

    Assumes Python v2.6+

    Usage
        -c --config         Configuration file (json format)
                            default: None
        -r --redis          Redis server connection string
                            default: localhost:6379
           --redisdb        Redis database ID
                            default: 9
        -d --debug          Turn on debug logging
                            default: False
        -l --logpath        Path where the log file output is written
                            default: None

    Sample Configuration file

        { 'debug': True,
          'logpath': '.'
        }

    Authors:
        bear    Mike Taylor <bear@mozilla.com>
"""

import os, sys
import time
import logging

from datetime import datetime

from flask import Flask, request
import redis


_redis = redis.StrictRedis(host='127.0.0.1', db=9)
app    = Flask(__name__)



@app.route('/briarpatch/v1/nagios', methods=['GET', 'POST'])
def nagios():
    if request.method == 'POST':
        dNow   = datetime.now()
        dToday = dNow.strftime('%Y-%m-%d')
        dHour  = dNow.strftime('%H')
        ts     = dNow.strftime('%Y%m%d%H%M%s')
        setKey = 'nagios:%s.%s' % (dToday, dHour)

        try:
            print request.form.keys()[0]
            d = json.loads('{%s"}' % request.form.keys()[0])
            print d

            if 'hostname' in d:
                hostKey = 'nagios:%s' % d['hostname']
                hashKey = '%s:%s'     % (hostKey, ts)

                # keep past 99 events for a host
                _redis.lpush(hostKey, hashKey)
                _redis.ltrim(hostKey, 0, 99)

                _redis.sadd(setKey, hashKey)
                _redis.expire(setKey, 604800) # 7 days

                for key in d:
                    _redis.hset(hashKey, key, d[key])

                _redis.expire(hashKey, 604800) # 7 days
        finally:
            return 'ok'
    else:
        return 'POST only please'

@app.route('/')
def index_page():
    return 'bpNagios'

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

