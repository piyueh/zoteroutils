#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.
"""Basic operations on Zotero's SQLite database.

Functions in this module are those related to reading data from SQLite database.
"""
# standard libraries
from pathlib import Path as _Path
from typing import List as _List
from typing import Sequence as _Sequence
from typing import Callable as _Callable
from typing import Union as _Union

# additionally installed libraries
import pandas
from sqlalchemy.engine import Connection as ConnType  # for type hinting

# a type hint for path-like object
_PathLike = _Union[str, _Path]


def get_table_names(conn: ConnType) -> _List[str]:
    """Returns the names of all tables in the database.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.

    Returns
    -------
    pandas.Series
       The names of the tables in this database.

    """

    names: pandas.DataFrame = pandas.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table';", conn)

    return names["name"].to_list()


def get_table_fields(conn: ConnType, table: str, info: _Sequence[str] = None) -> _List[str]:
    """Returns the names of the fields (columns) in a table.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.
    table : str
        The name of the table.
    info: list/tuple of strings
        The information to read. Default only read the names of the columns of the table. Available
        options are: "cid", "name", "type", "notnull", "dflt_value", and "pk"

    Returns
    -------
    1. list of str
        If the name is the only info to read, then returns a list of the names of the columns.
    2. pandas.DataFrame
        If other information is also read, returns a pandas.DataFrame.
    """
    fields: pandas.DataFrame = pandas.read_sql_query(
        "PRAGMA table_info('{0}');".format(table), conn)

    if info is None:
        return fields["name"].to_list()

    return fields[info]


def get_item_types_mapping(conn: ConnType):
    """Returns dicts of the mappings bewtween document type names and type ids.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.

    Returns
    -------
    doctype2id : dict of (str, int)
        Mapping from document type names to integer ids.
    id2doctype : dict of (int, str)
        Mapping from document type ids to string names.
    """
    temp = pandas.read_sql_table("itemTypes", conn, columns=["typeName", "itemTypeID"])
    doctype2id = temp.set_index("typeName")["itemTypeID"].to_dict()
    id2doctype = temp.set_index("itemTypeID")["typeName"].to_dict()
    return doctype2id, id2doctype


def get_field_names_mapping(conn: ConnType):
    """Returns dicts of the mappings bewtween field names and field ids.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.

    Returns
    -------
    field2id : dict of (str, int)
        Mapping from field names to integer ids.
    id2field : dict of (int, str)
        Mapping from field ids to string names.
    """
    temp = pandas.read_sql_table("fieldsCombined", conn, columns=["fieldID", "fieldName"])
    field2id = temp.set_index("fieldName")["fieldID"].to_dict()
    id2field = temp.set_index("fieldID")["fieldName"].to_dict()
    return field2id, id2field


def get_creator_types_mapping(conn: ConnType):
    """Returns dicts of the mappings bewtween creater type names and field ids.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.

    Returns
    -------
    creatortype2id : dict of (str, int)
        Mapping from creater type names to integer ids.
    id2creatortype : dict of (int, str)
        Mapping from creater type id to string names.
    """
    temp = pandas.read_sql_table("creatorTypes", conn, columns=["creatorTypeID", "creatorType"])
    creatortype2id = temp.set_index("creatorType")["creatorTypeID"].to_dict()
    id2creatortype = temp.set_index("creatorTypeID")["creatorType"].to_dict()
    return creatortype2id, id2creatortype


def get_all_names(conn: ConnType, split=False):
    """Gets the full names of all the authors in the database.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.
    split : bool, optional
        Whether to return full names or split last and first names.

    Returns
    -------
    If split is False (default), it returns a pandas.Series of full names. Otherwise, it returns
    a pandas.DataFrame with two columns: "last name" and "first name".
    """
    data: pandas.DataFrame = pandas.read_sql("creators", conn, columns=["firstName", "lastName"])

    if split:
        data: pandas.DataFrame = data.rename(
            columns={"firstName": "first name", "lastName": "last name"})
    else:
        data["firstName"] += " "
        data: pandas.Series = (data["firstName"] + data["lastName"]).str.strip()
        data.name = "full name"

    return data


def _query_factory(
    query: str,
    org_tag: str,
    new_tag: str,
    after: _Callable[[pandas.Series], pandas.Series] = lambda x: x) \
        -> _Callable[[ConnType], pandas.Series]:
    """A factory that creates a function doing simple querying and return a Series.

    This is mainly for internal use.

    Parameters
    ----------
    query : str
    org_tag : str
    new_tag : str
    after : None or callable with a signature (pandas.Series) -> pandas.Series

    Returns
    -------
    callable/function
        A function with a signature of `(sqlachemy.engine.Connection) -> pandas.Series`.
    """

    def func(conn: ConnType, **mapping) -> pandas.Series:
        """Returns a list of all items' {0}s.

        Note
        ----
        1. Items with "attachment" type are ignored.
        2. Only return items with non-NaN values.

        Parameters
        ----------
        conn : sqlalchemy.engine.Connection
            The connection object to the database.
        **mapping : keyword-values
            The mapping from required keys to values used in query strings.

        Returns
        -------
        pandas.Dataframe
            All items' {0}s, where the indices are the `itemID`s, and it only has one column.
        """

        results: pandas.DataFrame = pandas.read_sql_query(query.format(**mapping), conn)
        results: pandas.DataFrame = results.set_index("itemID").rename({org_tag: new_tag}, axis=1)
        results: pandas.DataFrame = after(results)
        return results

    func.__doc__ = func.__doc__.format(new_tag)  # replace the placeholder in the docstring
    func.query = query  # make a copy of the query string

    return func


# a function to get the document types of all documents
get_all_doc_types: _Callable[[ConnType, int], pandas.Series] = _query_factory(
    """
        SELECT items.itemID, itemTypes.typeName
        FROM items, itemTypes
        WHERE
            items.itemTypeID <> {attachment} AND
            items.itemTypeID <> {note} AND
            itemTypes.itemTypeID = items.itemTypeID
    """,
    "typeName", "document type"
)

# a function to get the document titles of all documents
get_all_doc_titles: _Callable[[ConnType, int], pandas.Series] = _query_factory(
    """
        SELECT items.itemID, itemDataValues.value
        FROM items, itemData, itemDataValues
        WHERE
            items.itemTypeID <> {attachment} AND
            items.itemTypeID <> {note} AND
            itemData.itemID = items.itemID AND
            itemData.fieldID = {title} AND
            itemDataValues.valueID = itemData.valueID
    """,
    "value", "title"
)

# a function to get the publication titles of all documents
get_all_doc_publications: _Callable[[ConnType, int], pandas.Series] = _query_factory(
    """
        SELECT items.itemID, itemDataValues.value
        FROM items, itemData, itemDataValues
        WHERE
            items.itemTypeID <> {attachment} AND
            items.itemTypeID <> {note} AND
            itemData.itemID = items.itemID AND (
                itemData.fieldID IN  ({publicationTitle}, {encyclopediaTitle}, {dictionaryTitle}) OR
                itemData.fieldID IN  ({websiteTitle}, {forumTitle}, {blogTitle}) OR
                itemData.fieldID IN  ({proceedingsTitle}, {bookTitle}, {programTitle})
            ) AND
            itemDataValues.valueID = itemData.valueID
    """,
    "value", "publication title"
)
"""
"""

# a function to get the publish years of all documents
get_all_doc_years: _Callable[[ConnType, int], pandas.Series] = _query_factory(
    """
        SELECT items.itemID, itemDataValues.value
        FROM items, itemData, itemDataValues
        WHERE
            items.itemTypeID <> {attachment} AND
            items.itemTypeID <> {note} AND
            itemData.itemID = items.itemID AND
            itemData.fieldID = {date} AND
            itemDataValues.valueID = itemData.valueID
    """,
    "value", "year",
    lambda x: x.replace(r"(?P<year>\d{4}).*", r"\g<year>", regex=True)
)

# a function to get the date of when each doc was added to the database
get_all_doc_added_dates: _Callable[[ConnType, int], pandas.Series] = _query_factory(
    "SELECT items.itemID, items.dateAdded FROM items\n" +
    "WHERE items.itemTypeID <> {attachment} AND items.itemTypeID <> {note};",
    "dateAdded", "time added"
)


def get_all_doc_authors(conn: ConnType, attachment: int, note: int, author: int, **kwargs: int):
    """Returns the last names of the authors of all documents.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.
    attachment : int
        The ID of the item type *attachment*.
    note : int
        The ID of the item type *note*.
    author : int
        The ID of the creator type *author*.
    **kwargs : int
        Not used. Just to conform the signature with other similar functions.

    Returns
    -------
    pandas.DataFrame
        The values in the only one column are lists of strings of last names. The indices of are
        "itemID"s.
    """
    # pylint: disable=unused-argument

    query = """
        SELECT items.itemID, itemCreators.orderIndex, creators.lastName
        FROM items, itemCreators, creators
        WHERE
            items.itemTypeID <> {attachment} AND
            items.itemTypeID <> {note} AND
            itemCreators.itemID = items.itemID AND
            itemCreators.creatorTypeID = {author} AND
            creators.creatorID = itemCreators.creatorID
    """.format(attachment=attachment, note=note, author=author)

    results: pandas.DataFrame = pandas.read_sql_query(query, conn)
    results: pandas.DataFrame = results.sort_values(["itemID", "orderIndex"])
    results: pandas.DataFrame = results.set_index("itemID").drop(columns="orderIndex")
    results: pandas.core.groupby.DataFrameGroupBy = results.groupby(level=0)
    results: pandas.DataFrame = results.aggregate(lambda x: x.values.tolist())
    results: pandas.DataFrame = results.rename(columns={"lastName": "author"})

    return results


def get_all_doc_attachments(conn: ConnType, attachment: int, prefix: _PathLike = "", **kwargs: int):
    """Returns the paths to the attachments to all documents.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The connection object to the database.
    attachment : int
        The ID of the item type *attachment*.
    prefix : str, pathlib.Path, or path-like
        The path prefix to prepend.
    **kwargs : int
        Not used. Just to conform the signature with other similar functions.

    Returns
    -------
    pandas.DataFrame
        The values in the only one column are the relative paths to attachments. The indices of are
        "itemID"s. The relative paths are relative to the Zotero storage path.
    """
    # pylint: disable=unused-argument

    query = """
        SELECT itemAttachments.parentItemID, items.key, itemAttachments.path
        FROM items, itemAttachments
        WHERE
            items.itemTypeID = {attachment} AND
            itemAttachments.itemID = items.itemID
    """.format(attachment=attachment)

    results: pandas.DataFrame = pandas.read_sql_query(query, conn)
    results: pandas.DataFrame = results.rename(columns={"parentItemID": "itemID"})
    results: pandas.DataFrame = results.set_index("itemID").dropna(0, subset=["path"])

    prefix = _Path(prefix)
    results["key"] = results["key"].map(prefix.joinpath)
    results["path"] = results["path"].str.replace("storage:", "")
    results: pandas.Series = results.apply(lambda x: x["key"].joinpath(x["path"]), 1)
    results: pandas.core.groupby.SeriesGroupBy = results.groupby(level=0)
    results: pandas.Series = results.aggregate(lambda x: x.values.tolist())
    results: pandas.DataFrame = results.to_frame("attachment path")

    return results
