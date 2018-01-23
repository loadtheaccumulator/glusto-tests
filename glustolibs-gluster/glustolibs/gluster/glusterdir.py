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
"""Description: Module for library gluster file class and related functions"""

from glusto.core import Glusto as g

from glustolibs.gluster.glusterfile import GlusterFile


def mkdir(host, fqpath, parents=False, mode=None):
    """Create a directory or path of directories.

    Args:
        host (str): The hostname/ip of the remote system.
        fqpath (str): The fully-qualified path to the file.
        parents (Bool): create parent directories if they do not exist.
        mode (str): the initial mode of the directory.

    Returns:
        True on success.
        False on failure.
    """
    command_list = ['mkdir']
    if parents:
        command_list.append('-p')
    if mode is not None:
        command_list.append('-m %s' % mode)
    command_list.append(fqpath)
    rcode, _, _ = g.run(host, ' '.join(command_list))

    if rcode == 0:
        return True

    return False


def rmdir(host, fqpath, force=False):
    """Remove a directory"""
    command_list = ['rmdir']
    if force:
        command_list = ["rm"]
        command_list.append('-rf')
    command_list.append(fqpath)
    rcode, _, _ = g.run(host, ' '.join(command_list))

    if rcode == 0:
        return True

    return False


class GlusterDir(GlusterFile):
    """Class to handle directories specific to Gluster (client and backend)"""
    def mkdir(self, parents=False, mode=None):
        """mkdir the instance on the remote host

        Args:
            parents (Bool): create parent directories if they do not exist.
            mode (str): the initial mode of the directory.

        Returns:
            True on success.
            False on failure.
        """
        if not self.exists_on_client:
            ret = mkdir(self._host, self._fqpath, parents, mode)
            if ret:
                return True

        return False

    def create(self):
        """Creates the directory and parent directories if they do not exist.
            Overrides GlusterFile.create() to handle directories.
        Args:
            None

        Returns:
            True on success.
            False on failure.
        """
        return self.mkdir(parents=True, mode=None)
