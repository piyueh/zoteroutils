#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""A tirvial dict-like object in which keys can be accessed ad attributes."""
from typing import Any as _Any
from collections import UserDict as _UserDict


class DummyDict(_UserDict):  # pylint: disable=too-many-ancestors
    """A tirvial dict-like object in which keys can be accessed ad attributes."""

    def __setattr__(self, name: str, value: _Any):
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
