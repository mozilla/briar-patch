#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import requests

from multiprocessing import get_logger

log = get_logger()

buildapi_url = "http://buildapi01.build.scl1.mozilla.com/buildapi/"

def json_get(url):
    """ Fetch a JSON document and return the corresponding Python data
        structure. """
    log.debug("Fetching '%s'" % url)
    r = requests.get(url)
    if r.status_code != requests.codes.ok:
        r.raise_for_status()
    return r.json()

def recent_builds(slavename, limit=20):
    """ Return at most 'limit' most recent builds for the given build slave.
    """
    return json_get("%s/recent/%s?format=json&numbuilds=%i" % (buildapi_url, slavename, limit))

def last_build_endtime(slavename):
    """ Returns a UNIX timestamp of when the most recent build finished
        for the given build slave.  Returns None if there are no builds. """
    rb = recent_builds(slavename, limit=1)
    if rb is not None and type(rb) == list and len(rb) > 0:
        return rb[0]['endtime']
    return None

if __name__ == '__main__':
    # test
    rb = last_build_endtime('talos-r3-w7-029')
    assert(rb is not None)
    assert(int(rb))
    import pprint
    pprint.pprint(rb)

