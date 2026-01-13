#!/bin/sh

mlogd=$(which mlogd)
if [ "x$mlogd" = "x" ]; then
    echo "ERROR: mlogd not found in PATH" 1>&2
    exit 1
fi

cwd=$(pwd)
rm -rf log
mkdir log
path="$cwd/log/test.log"

exec mlogd --flush $path
