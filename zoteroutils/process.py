#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""Functions that process data."""
from typing import Sequence as _Sequence


def authors_agg(data: _Sequence[str]) -> str:
    """Combine a list of authors's last names based on the numbers of authors.

    No author => ""
    One author => the author's last name
    Two authors => first authors's last name and the second author's last name
    Three and more authors => first author's last name et al.

    Parameters
    ----------
    data : list-like
        A list of authors' last names.

    Returns
    -------
    A str.
    """

    if len(data) == 0:
        return ""

    if len(data) == 1:
        return data[0]

    if len(data) == 2:
        return "{0[0]} and {0[1]}".format(data)

    return "{} et al.".format(data[0])
