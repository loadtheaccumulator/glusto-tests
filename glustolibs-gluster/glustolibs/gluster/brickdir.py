#!/usr/bin/env python
#  Copyright (C) 2018 Red Hat, Inc. <http://www.redhat.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
"""Module for library brick class and related functions"""

import os

from glusto.core import Glusto as g


def get_hashrange(brickdir_path):
    """Get the hex hash range for a brick

    Args:
        brickdir_url (str): path of the directory as returned from pathinfo
        (e.g., server1.example.com:/bricks/brick1/testdir1)

    Returns:
        list containing the low and high hash for the brickdir
        None on fail
    """
    (host, fqpath) = brickdir_path.split(':')

    command = ("getfattr -n trusted.glusterfs.dht -e hex %s "
               "2> /dev/null | grep -i trusted.glusterfs.dht | "
               "cut -d= -f2" % fqpath)
    rcode, rout, _ = g.run(host, command)
    full_hash_hex = rout.strip()

    if rcode == 0:
        # Grab the trailing 16 hex bytes
        trailing_hash_hex = full_hash_hex[-16:]
        # Split the full hash into low and high
        hash_range_low = int(trailing_hash_hex[0:8], 16)
        hash_range_high = int(trailing_hash_hex[-8:], 16)

        return (hash_range_low, hash_range_high)

    return None


def file_exists(host, filename):
    """Check if file exists at path on host

    Args:
        host (str): hostname or ip of system
        filename (str): fully qualified path of file

    Returns:
        True if file exists
        False if file does not exist
    """
    command = "ls -ld %s" % filename
    rcode, _, _ = g.run(host, command)
    if rcode == 0:
        return True

    return False


class BrickDir(object):
    """Directory on a brick"""
    def __init__(self, path):
        self._path = path
        (self._host, self._fqpath) = self._path.split(':')
        self._hashrange = None
        self._hashrange_low = None
        self._hashrange_high = None

    def _get_hashrange(self):
        """get the hash range for a brick from a remote system"""
        self._hashrange = get_hashrange(self._path)
        self._hashrange_low = self._hashrange[0]
        self._hashrange_high = self._hashrange[1]

    @property
    def path(self):
        """The brick url
        Example:
            server1.example.com:/bricks/brick1
        """
        return self._path

    @property
    def host(self):
        """The hostname/ip of the system hosting the brick"""
        return self._host

    @property
    def fqpath(self):
        """The fully qualified path of the brick directory"""
        return self._fqpath

    @property
    def hashrange(self):
        """A list containing the low and high hash of the brick hashrange"""
        if self._hashrange is None:
            g.log.info("Retrieving hash range for %s" % self._path)
            self._get_hashrange()

        return (self._hashrange_low, self._hashrange_high)

    @property
    def hashrange_low(self):
        """The low hash of the brick hashrange"""
        if self.hashrange is None or self._hashrange_low is None:
            self._get_hashrange()

        return self._hashrange_low

    @property
    def hashrange_high(self):
        """The high hash of the brick hashrange"""
        if self.hashrange is None or self._hashrange_high is None:
            self._get_hashrange()

        return self._hashrange_high

    def hashrange_contains_hash(self, filehash):
        """Check if a hash number falls between the brick hashrange

        Args:
            filehash (int): hash being checked against range

        Returns:
            True if hash is in range
            False if hash is out of range
        """
        if self._hashrange is None:
            self._get_hashrange()

        if self._hashrange_low <= filehash <= self._hashrange_high:
            return True

        return False

    def has_zero_hashrange(self):
        """figure out if the brickdir has a low and high zero value hash"""
        if self.hashrange_low == 0 and self.hashrange_high == 0:
            return True

        return False

    def resync_hashrange(self):
        """Reset the hashrange attributes and update hashrange from brick
        Args:
            None

        Returns:
            None
        """
        self._hashrange = None
        self._hashrange_low = None
        self._hashrange_high = None
        self._get_hashrange()

    def file_exists(self, filename):
        """Check if the file exists on the brick

        Args:
            filepath (int): relative path of the file

        Returns:
            True if the file exists on the brick
            False if the file does not exist on the brick
        """
        fqfilepath = os.path.join(self._fqpath, filename)

        if file_exists(self._host, fqfilepath):
            return True

        return False
