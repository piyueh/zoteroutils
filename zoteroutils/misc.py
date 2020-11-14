#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 Pi-Yueh Chuang <pychuang@pm.me>
#
# Distributed under terms of the BSD 3-Clause license.

"""SQL query templates."""
from .dummy_dict import DummyDict

# some useful(?) SQL query text
queries = DummyDict()
queries.table_names = "SELECT name FROM sqlite_master WHERE type='table'"
queries.table_columns = "PARGMA table_info({0})"
queries.type_ids = "SELECT itemTypeID FROM itemTypes WHERE typeName IN ({0})"
queries.type_map = "SELECT typeName, itemTypeID FROM itemTypes WHERE typeName IN ({0})"
queries.field_ids = "SELECT fieldID FROM fields WHERE fieldName IN ({0})"
queries.field_map = "SELECT fieldName, fieldID FROM fields WHERE fieldName IN ({0})"
queries.items_types = "SELECT * FROM items WHERE itemID IN ({0}) AND itemTypeID IN ({1})"
queries.items_not_types = "SELECT * FROM items WHERE itemID IN ({0}) AND itemTypeID NOT IN ({1})"
queries.all_items_types = "SELECT * FROM items WHERE itemTypeID IN ({0})"
queries.all_items_not_types = "SELECT * FROM items WHERE itemTypeID NOT IN ({0})"
queries.itemdata_fields = "SELECT * FROM itemData WHERE itemID In ({0}) AND fieldID IN ({1})"
queries.all_itemdata_fields = "SELECT * FROM itemData WHERE fieldID IN ({0})"

# short hand for frequently used combination when requesting brief information
short_info = DummyDict()
short_info.fields = [
    "date", "title", 'publicationTitle', 'encyclopediaTitle', 'dictionaryTitle', 'websiteTitle',
    'forumTitle', 'blogTitle', 'proceedingsTitle', 'bookTitle', 'programTitle'
]
short_info.meta = ["itemTypeID", "dateAdded"]
short_info.names = ["lastName"]
short_info.mapping = {
    "date": "year", "title": "document title", 'publicationTitle': "publication title",
    'encyclopediaTitle': "publication title", 'dictionaryTitle': "publication title",
    'websiteTitle': "publication title", 'forumTitle': "publication title",
    'blogTitle': "publication title", 'proceedingsTitle': "publication title",
    'bookTitle': "publication title", 'programTitle': "publication title",
    "dateAdded": "time added", "typeName": "document type",
    "lastName": "last name", "firstName": "first name",
}
