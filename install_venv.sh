#!/bin/bash

if [ -z $1 ]; then
  echo "please specify a target directory to install into"
  echo "usage: $0 target"
  exit 1
fi

TARGET=$1
PYTHON_EXE=python2.7

mkdir ${TARGET}

if [ ! -d $TARGET ]; then
  echo "unable to create ${TARGET}"
  exit 2
fi

set -x

virtualenv ${TARGET} --python=${PYTHON_EXE}

cd ${TARGET}

if [ -e bin/activate ]; then
  . bin/activate
  python -V

  pip install ssh
  pip install boto
  pip install redis
  pip install requests
  pip install dnspython

  hg clone http://hg.mozilla.org/build/tools

  TOOLS=`pwd`/tools

  git clone git://github.com/mozilla/briar-patch.git

  ln -s briar-patch/releng .
  ln -s briar-patch/kittenherder.py .
  ln -s briar-patch/kitten.py .

  echo "{ \"ldapuser\": \"no_ldap\", \"tools\": \"${TOOLS}\"}" > ./kittenherder.cfg
  cp kittenherder.cfg kitten.cfg

  echo "The virtualenv inside of ${TARGET} is setup.  Please remember to source bin/activite before using"
  echo "Your keystore is set to \"memory\" currently, you will be prompted for the cltbld password each run"
fi

