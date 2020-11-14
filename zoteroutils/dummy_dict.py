#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""A tirvial dict-like object in which keys can be accessed ad attributes."""
import typing
from collections import UserDict


class DummyDict(UserDict):  # pylint: disable=too-many-ancestors
    """A tirvial dict-like object in which keys can be accessed as attributes."""

    def __setattr__(self, name: str, value: typing.Any):
        try:
            super().__setitem__(name, value)
        except RecursionError:
            super().__setattr__(name, value)

    def __getattr__(self, name: str):
        try:
            return super().__getitem__(name)
        except KeyError as err:
            raise AttributeError from err

    def __delattr__(self, name: str):
        try:
            super().__delitem__(name)
        except KeyError as err:
            raise AttributeError from err

    def __str__(self):
        s = "Type: zoteroutils.misc.DummyDict"

        for k, v in self.data.items():
            s += "\n    {0}: {1}".format(str(k), str(v))
        return s
