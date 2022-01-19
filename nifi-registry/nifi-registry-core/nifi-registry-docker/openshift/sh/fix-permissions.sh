#!/bin/sh

# Allow this script to fail without failing a build
set +e

SYMLINK_OPT=${2:--L}

# Fix permissions on the given directory or file to allow group read/write of
# regular files and execute of directories.

[ $(id -u) -ne 0 ] && CHECK_OWNER=" -uid $(id -u)"

# If argument does not exist, script will still exit with 0,
# but at least we'll see something went wrong in the log
if ! [ -e "$1" ] ; then
  echo "ERROR: File or directory $1 does not exist." >&2
  # We still want to end successfully
  exit 0
fi

find $SYMLINK_OPT "$1" ${CHECK_OWNER} \! -gid 0 -exec chgrp 0 {} +
find $SYMLINK_OPT "$1" ${CHECK_OWNER} \! -perm -g+rw -exec chmod g+rw {} +
find $SYMLINK_OPT "$1" ${CHECK_OWNER} -perm /u+x -a \! -perm /g+x -exec chmod g+x {} +
find $SYMLINK_OPT "$1" ${CHECK_OWNER} -type d \! -perm /g+x -exec chmod g+x {} +

# Always end successfully
exit 0