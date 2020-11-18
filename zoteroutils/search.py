#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""Functions related to text searching."""
import typing
import pandas
import sqlalchemy


def search_author_simple(conn: sqlalchemy.engine.Connection, key: str) -> pandas.DataFrame:
    """Search a single name from the author list.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The SQLite connection through `sqlachemy`.
    key : str
        The key word to search for.

    Returns
    -------
    pandas.DataFrame
        A dataframe of `itemID`s.
    """

    query = """
        SELECT DISTINCT itemID
        FROM (SELECT creatorID FROM creators WHERE (
            creators.firstName LIKE '%{0}%' OR
            creators.lastName LIKE '%{0}%')
        )
        INNER JOIN itemCreators USING(creatorID)
    """.format(key)

    results = pandas.read_sql_query(query, conn)
    return results


def search_fields_simple(
    conn: sqlalchemy.engine.Connection,
    key: str,
    ignored_types: typing.Sequence[str] = ("attachment", "note")
) -> pandas.DataFrame:
    """Search a single key word in items' fields.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The SQLite connection through `sqlachemy`.
    key : str
        The key word to search for.
    ignored_type : list-like of strings
        Item types to be ignored. Default to ignore attachments and notes.

    Returns
    -------
    pandas.DataFrame
        A dataframe of `itemID`s.
    """
    ignored_types = ", ".join(map("'{}'".format, ignored_types))

    query = """
        SELECT DISTINCT itemID
        FROM (
            SELECT itemID
            FROM items
            INNER JOIN (
                SELECT itemTypeID FROM itemTypes WHERE typeName NOT IN ({0})
            ) USING(itemTypeID)
        )
        INNER JOIN (
            SELECT DISTINCT itemID
            FROM itemData
            INNER JOIN (
                SELECT valueID FROM itemDataValues WHERE value LIKE '%{1}%'
            ) USING(valueID)
        ) USING(itemID)
    """.format(ignored_types, key)

    results = pandas.read_sql_query(query, conn)
    return results


def search_full_texts_simple(conn: sqlalchemy.engine.Connection, key: str) -> pandas.DataFrame:
    """Search a single key word in items' attachments.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        The SQLite connection through `sqlachemy`.
    key : str
        The key word to search for.

    Returns
    -------
    pandas.DataFrame
        A dataframe of `itemID`s.
    """

    query = """
        SELECT parentItemID AS itemID
        FROM itemAttachments
        INNER JOIN (
            SELECT DISTINCT itemID
            FROM fulltextItemWords
            INNER JOIN (SELECT wordID FROM fulltextWords WHERE word LIKE '%{0}%') USING(wordID)
        ) USING(itemID)
    """.format(key)

    results = pandas.read_sql_query(query, conn)
    return results


def search_authors(
    conn: sqlalchemy.engine.Connection,
    keys: str,
    item_ids: typing.Optional[typing.Sequence[typing.Union[str, int]]] = None
) -> pandas.DataFrame:
    """Using full-text search table to search in authors' names. Allow searching multiple words.

    Notes
    -----
    May not be efficient if the database is very huge.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        Connection object from `sqlalchemy`.
    keys : str
        A single string containing all tokens/keys. Tokens/keys are separated by spaces.
    item_ids : None or a list-like of int/str
        Limit the candidate items to these item_ids. If None, search all items.

    Returns
    -------
    pandas.DataFrame
        A dataframe with only `itemID`s.
    """

    # delete residual table from past failed operations
    conn.execute("DROP TABLE IF EXISTS temp.searchable;")

    keys = " ".join(["\"{}\"".format(key) for key in keys.split()])

    if item_ids is None:
        partial_table = "itemCreators"
    else:
        item_ids = ", ".join(map(str, item_ids))
        partial_table = "(SELECT * FROM itemCreators WHERE itemID IN ({0}))".format(item_ids)

    # create a temporary FTS table
    conn.execute(
        "CREATE VIRTUAL TABLE temp.searchable USING FTS5(itemID UNINDEXED, firstName, lastName);"
    )

    # copy to the FTS table
    conn.execute(
        """INSERT INTO temp.searchable
            SELECT
                itemID, GROUP_CONCAT(firstName) AS firstNAme, GROUP_CONCAT(lastName) AS lastName
            FROM {0} INNER JOIN creators USING(creatorID) GROUP BY itemID;
        """.format(partial_table)
    )

    # conduct the search
    results = pandas.read_sql_query(
        """SELECT itemID FROM temp.searchable WHERE searchable MATCH '{0}' ORDER BY rank;
        """.format(keys),
        conn
    )

    # delete the temporary table
    conn.execute("DROP TABLE IF EXISTS temp.searchable;")

    return results


def search_fields(
    conn: sqlalchemy.engine.Connection,
    keys: str,
    item_ids: typing.Optional[typing.Sequence[typing.Union[str, int]]] = None,
) -> pandas.DataFrame:
    """Using full-text search table to search all fields. Allow searching multiple words.

    Notes
    -----
    May not be efficient if the database is very huge.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        Connection object from `sqlalchemy`.
    keys : str
        A single string containing all tokens/keys. Tokens/keys are separated by spaces.
    item_ids : None or a list-like of int/str
        Limit the candidate items to these item_ids. If None, search all items.

    Returns
    -------
    pandas.DataFrame
        A dataframe with only `itemID`s.
    """

    # delete residual table from past failed operations
    conn.execute("DROP TABLE IF EXISTS temp.searchable;")

    keys = " ".join(["\"{}\"".format(key) for key in keys.split()])

    if item_ids is None:
        partial_table = "itemData"
    else:
        item_ids = ", ".join(map(str, item_ids))
        partial_table = "(SELECT * FROM itemData WHERE itemID IN ({0}))".format(item_ids)

    # create a temporary FTS table
    conn.execute("CREATE VIRTUAL TABLE temp.searchable USING FTS5(itemID UNINDEXED, values);")

    # copy to the FTS table
    conn.execute(
        """INSERT INTO temp.searchable
            SELECT itemID, GROUP_CONCAT(value) as value FROM {0}
            INNER JOIN itemDataValues USING(valueID) GROUP BY itemID;
        """.format(partial_table)
    )

    # conduct the search
    results = pandas.read_sql_query(
        """SELECT itemID FROM temp.searchable WHERE searchable MATCH '{0}' ORDER BY rank;
        """.format(keys),
        conn
    )

    # delete the temporary table
    conn.execute("DROP TABLE IF EXISTS temp.searchable;")

    return results


def search_full_texts(
    conn: sqlalchemy.engine.Connection,
    keys: str,
    item_ids: typing.Optional[typing.Sequence[typing.Union[str, int]]] = None,
) -> pandas.DataFrame:
    """Using full-text search table to search all fields. Allow searching multiple words.

    Notes
    -----
    May not be efficient if the database is very huge.

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        Connection object from `sqlalchemy`.
    keys : str
        A single string containing all tokens/keys. Tokens/keys are separated by spaces.
    item_ids : None or a list-like of int/str
        Limit the candidate items to these item_ids. If None, search all items.

    Returns
    -------
    pandas.DataFrame
        A dataframe with only `itemID`s.
    """

    # delete residual tables from past operations
    conn.execute("DROP TABLE IF EXISTS searchable1;")
    conn.execute("DROP TABLE IF EXISTS temp.searchable2;")

    if item_ids is None:
        partial_table = "fulltextItemWords"
    else:  # if item_ids are provided, first get their attachments' IDs; then query a smaller table
        item_ids = ", ".join(map(str, item_ids))
        partial_table = """
            SELECT * FROM fulltextItemWords WHERE itemID IN (
                SELECT itemID
                FROM itemAttachments
                WHERE itemID IN ({0}) OR parentItemID IN ({0})
            )
        """.format(item_ids)

    keys = ["\"{}\"".format(key) for key in keys.split()]

    # the table fulltextWords is likely too big, so use FTS table to filter out unnecessary rows
    conn.execute(
        "CREATE VIRTUAL TABLE searchable1 USING FTS5("
        "wordID UNINDEXED, word, content=fulltextWords);"
    )

    # due to some unknon reason, we'll have to rebuild the index
    conn.execute("INSERT INTO searchable1(searchable1) VALUES('rebuild');")

    # create the query command that gives a smaller fulltext word table
    small_table = "SELECT * FROM searchable1 WHERE word MATCH '{0}'".format(" OR ".join(keys))

    # create a second FTS table to do the real search
    conn.execute("CREATE VIRTUAL TABLE temp.searchable2 USING FTS5(itemID UNINDEXED, word);")

    # insert the final candidate table to the second FTS table
    conn.execute(
        """INSERT INTO temp.searchable2
            SELECT itemID, GROUP_CONCAT(word) AS word FROM ({0})
            INNER JOIN ({1}) USING(wordID) GROUP BY itemID;
        """.format(partial_table, small_table)
    )

    # get the final DataFrame; return the parentItemIDs of the attachments
    results = pandas.read_sql_query(
        """SELECT parentItemID AS itemID FROM itemAttachments INNER JOIN (
            SELECT itemID FROM temp.searchable2 WHERE searchable2 MATCH '{0}') USING(itemID);
        """.format(" ".join(keys)),
        conn
    )

    # delete the tables
    conn.execute("DROP TABLE IF EXISTS searchable1;")
    conn.execute("DROP TABLE IF EXISTS temp.searchable2;")

    return results
