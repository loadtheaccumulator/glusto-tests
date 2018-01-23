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
"""Exceptions for Gluster libraries and tests"""


class GlusterException(Exception):
    """Base Gluster exception class."""
    def __init__(self, arg):
        Exception.__init__(self, arg)
        self.msg = arg


class TransportEndpointNotConnectedException(GlusterException):
    """Exception for transport endpoint not connected error."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class NoSuchFileOrDirectoryException(GlusterException):
    """Exception for no such file or directory error."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class AttributesDoNotMatchException(GlusterException):
    """Attributes do not match exception."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class LayoutIsNotCompleteException(GlusterException):
    """Exception raised when the layout of a file is not complete."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class LayoutIsNotBalancedException(GlusterException):
    """Exception raised when the layout of a file is not balanced."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class LayoutHasHolesException(GlusterException):
    """Exception raised when the layout of a file has holes."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class LayoutHasOverlapsException(GlusterException):
    """Exception raised when the layout of a file has overlaps."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class FileDoesNotExistOnHashedBricks(GlusterException):
    """Exception raised when a file/dir does not exist where it is hashed."""
    def __init__(self, arg):
        GlusterException.__init__(self, arg)
        self.msg = arg


class ConfigError(Exception):
    '''
    Custom exception thrown when there is an unrecoverable configuration error.
    For example, a required configuration key is not found.
    '''
    pass


class ExecutionError(Exception):
    '''
    Custom exception thrown when a command executed by Glusto results in an
    unrecoverable error.

    For example, all hosts are not in peer state or a volume canot be setup.

    '''
    pass
