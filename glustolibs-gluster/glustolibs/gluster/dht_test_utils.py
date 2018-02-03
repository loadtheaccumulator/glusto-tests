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
"""Module for library DHT test utility functions"""

import os

from glusto.core import Glusto as g

from glustolibs.gluster.glusterfile import GlusterFile
from glustolibs.gluster.glusterdir import GlusterDir
from glustolibs.gluster.layout import Layout
import glustolibs.gluster.constants as k
import glustolibs.gluster.exceptions as gex


def run_layout_tests(fqpath, layout, test_type):
    """run the is_complete and/or is_balanced tests"""
    if test_type & k.TEST_LAYOUT_IS_COMPLETE:
        g.log.info("Testing layout complete for %s" % fqpath)
        if not layout.is_complete:
            msg = ("Layout for %s IS NOT COMPLETE" % fqpath)
            g.log.error(msg)
            raise gex.LayoutIsNotCompleteException(msg)
    if test_type & k.TEST_LAYOUT_IS_BALANCED:
        g.log.info("Testing layout balance for %s" % fqpath)
        if not layout.is_balanced:
            msg = ("Layout for %s IS NOT BALANCED" % fqpath)
            g.log.error(msg)
            raise gex.LayoutIsNotBalancedException(msg)

    # returning True until logic requires non-exception error check(s)
    return True


def run_hashed_bricks_test(gfile):
    """run check for file/dir existence on brick based on calculated hash"""
    g.log.info("Testing file/dir %s existence on hashed brick(s)." %
               gfile.fqpath)
    if not gfile.exists_on_hashed_bricks:
        msg = ("File/Dir %s DOES NOT EXIST on hashed bricks." %
               gfile.fqpath)
        g.log.error(msg)
        raise gex.FileDoesNotExistOnHashedBricks(msg)

    return True


def validate_files_in_dir(host, rootdir,
                          file_type=k.FILETYPE_ALL,
                          test_type=k.TEST_ALL):
    """walk a directory tree and check if layout is_complete.

    Args:
        host (str): The host of the directory being traversed.
        rootdir (str): The fully qualified path of the dir being traversed.
        file_type (int): An or'd set of constants defining the file types
                        to test.
                            FILETYPE_DIR
                            FILETYPE_DIRS
                            FILETYPE_FILE
                            FILETYPE_FILES
                            FILETYPE_ALL

        test_type (int): An or'd set of constants defining the test types
                        to run.
                            TEST_LAYOUT_IS_COMPLETE
                            TEST_LAYOUT_IS_BALANCED
                            TEST_FILE_EXISTS_ON_HASHED_BRICKS
                            TEST_ALL

    Examples:
        # TEST LAYOUTS FOR FILES IN A DIRECTORY

        validate_files_in_dir(clients[0], '/mnt/glusterfs')
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              file_type=k.FILETYPE_DIRS)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              file_type=k.FILETYPE_FILES)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_COMPLETE,
                              file_type=(k.FILETYPE_DIRS | k.FILETYPE_FILES))
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_BALANCED)
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_LAYOUT_IS_BALANCED,
                              file_type=k.FILETYPE_FILES)

        # TEST FILES IN DIRECTORY EXIST ON HASHED BRICKS
        validate_files_in_dir(clients[0], '/mnt/glusterfs',
                              test_type=k.TEST_FILE_EXISTS_ON_HASHED_BRICKS)
    """
    layout_cache = {}

    conn = g.rpyc_get_connection(host)

    for walkies in conn.modules.os.walk(rootdir):
        g.log.info("TESTING DIRECTORY %s..." % walkies[0])

        # check directories
        if file_type & k.FILETYPE_DIR:
            for testdir in walkies[1]:
                fqpath = os.path.join(walkies[0], testdir)
                gdir = GlusterDir(host, fqpath)

                if gdir.parent_dir in layout_cache:
                    layout = layout_cache[gdir.parent_dir]
                else:
                    layout = Layout(gdir.parent_dir_pathinfo)
                    layout_cache[gdir.parent_dir] = layout

                    run_layout_tests(gdir.parent_dir, layout, test_type)

                if test_type & k.TEST_FILE_EXISTS_ON_HASHED_BRICKS:
                    run_hashed_bricks_test(gdir)

        # check files
        if file_type & k.FILETYPE_FILE:
            for file in walkies[2]:
                fqpath = os.path.join(walkies[0], file)
                gfile = GlusterFile(host, fqpath)

                if gfile.parent_dir in layout_cache:
                    layout = layout_cache[gfile.parent_dir]
                else:
                    layout = Layout(gfile.parent_dir_pathinfo)
                    layout_cache[gfile.parent_dir] = layout

                    run_layout_tests(gfile.parent_dir, layout, test_type)

                if test_type & k.TEST_FILE_EXISTS_ON_HASHED_BRICKS:
                    run_hashed_bricks_test(gfile)

    return True
