#!/bin/bash

set -e
FILTER=$1
FILTERBASE=$2

if [ "${FILTER}" == "" ]; then
    print "Usage: run_kittenherder.sh filter [filterbase]"
    exit
fi

if [ "${FILTERBASE}" == "" ]; then
  FILTERBASE="^%s"
fi

KITTEN=/home/buildduty/briar-patch
LOCK=${KITTEN}/lockfile_kittenherder_${FILTER}.cron
lockfile -5 -r 1 ${LOCK} || exit
trap "rm -f $LOCK" EXIT

cd ${KITTEN}
. bin/activate
nice python kittenherder.py --force --debug --filterbase ${FILTERBASE} -f ${FILTER} -v -l ${KITTEN}/logs > ${KITTEN}/logs/lastrun_kittenherder_${FILTER}.log 2>&1

