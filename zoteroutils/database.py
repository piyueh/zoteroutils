#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""A class that represents the SQLite database."""


class Database:
    """A class that represents a Zotero's SQLite database."""
    # pylint: disable=import-outside-toplevel, relative-beyond-top-level

    def __init__(self, zotero_dir: str):
        from pathlib import Path
        from .read import get_item_types_mapping, get_field_names_mapping, get_creator_types_mapping
        from .dummy_dict import DummyDict
        from sqlalchemy import create_engine

        # initialize information regarding paths
        self._paths: DummyDict = DummyDict()
        self._paths.dir: Path = Path(zotero_dir).expanduser().resolve()
        self._paths.db: Path = self._paths.dir.joinpath("zotero.sqlite")
        self._paths.storage: Path = self._paths.dir.joinpath("storage")

        # engine
        self._engine = create_engine("sqlite:///"+str(self._paths.db))

        # frequently used mappings
        self._maps = DummyDict()
        with self._engine.connect() as conn:
            self._maps.doctype2id, self._maps.id2doctype = get_item_types_mapping(conn)
            self._maps.field2id, self._maps.id2field = get_field_names_mapping(conn)
            self._maps.creatortype2id, self._maps.id2creatortype = get_creator_types_mapping(conn)

    @property
    def db(self):  # pylint: disable=invalid-name
        """The path to underlying SQLite database."""
        return self._paths.db

    @property
    def dir(self):
        """The path to the folder of Zotero data."""
        return self._paths.dir

    @property
    def storage(self):
        """The path to the folder of attachments."""
        return self._paths.storage

    @property
    def engine(self):
        """The underlying sqlalchemy.engine.Engine."""
        return self._engine

    @property
    def doctype2id(self):
        """A dict of (str, int) of the mapping between document type names -> type id."""
        return self._maps.doctype2id

    @property
    def id2doctype(self):
        """A dict of (int, str) of the mapping between document type id -> type name."""
        return self._maps.id2doctype

    @property
    def field2id(self):
        """A dict of (str, int) of the mapping between document field names -> field id."""
        return self._maps.field2id

    @property
    def id2field(self):
        """A dict of (int, str) of the mapping between document field id -> field name."""
        return self._maps.id2field

    @property
    def creatortype2id(self):
        """A dict of (str, int) of the mapping between creator type names -> integer id."""
        return self._maps.creatortype2id

    @property
    def id2creatortype(self):
        """A dict of (int, str) of the mapping between creator type id -> string name."""
        return self._maps.id2creatortype

    def get_all_docs(self, abs_attach_path=True, simplify_author=True):
        """A pandas.Dataframe of all documents with brief information.

        Parameters
        ----------
        abs_attach_path : bool
            Whether to use absolute paths for attachment paths. If false, the paths are relative
            to the Zotero data directory.

        Returns
        -------
        pandas.DataFrame
            A dataframe containing all items (except items with itemTypes of note and attachment).
        """
        from . import read
        from . import process
        with self._engine.connect() as conn:
            types = read.get_all_doc_types(conn, **self.doctype2id)
            titles = read.get_all_doc_titles(conn, **self.doctype2id, **self.field2id)
            pubs = read.get_all_doc_publications(conn, **self.doctype2id, **self.field2id)
            years = read.get_all_doc_years(conn, **self.doctype2id, **self.field2id)
            added = read.get_all_doc_added_dates(conn, **self.doctype2id, **self.field2id)
            authors = read.get_all_doc_authors(conn, **self.doctype2id, **self.creatortype2id)

            if abs_attach_path:
                atts = read.get_all_doc_attachments(conn, prefix=self.storage, **self.doctype2id)
            else:
                atts = read.get_all_doc_attachments(conn, **self.doctype2id)

        results = authors.join([types, titles, pubs, years, added, atts], None, "outer").fillna("")

        if simplify_author:
            results["author"] = results["author"].map(process.authors_agg)
        return results
