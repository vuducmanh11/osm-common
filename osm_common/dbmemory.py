# -*- coding: utf-8 -*-

# Copyright 2018 Telefonica S.A.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from osm_common.dbbase import DbException, DbBase
from osm_common.dbmongo import deep_update
from http import HTTPStatus
from uuid import uuid4
from copy import deepcopy

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"


class DbMemory(DbBase):

    def __init__(self, logger_name='db', lock=False):
        super().__init__(logger_name, lock)
        self.db = {}

    def db_connect(self, config):
        """
        Connect to database
        :param config: Configuration of database
        :return: None or raises DbException on error
        """
        if "logger_name" in config:
            self.logger = logging.getLogger(config["logger_name"])
        master_key = config.get("commonkey") or config.get("masterpassword")
        if master_key:
            self.set_secret_key(master_key)

    @staticmethod
    def _format_filter(q_filter):
        db_filter = {}
        # split keys with ANYINDEX in this way:
        # {"A.B.ANYINDEX.C.D.ANYINDEX.E": v }  -> {"A.B.ANYINDEX": {"C.D.ANYINDEX": {"E": v}}}
        if q_filter:
            for k, v in q_filter.items():
                db_v = v
                kleft, _, kright = k.rpartition(".ANYINDEX.")
                while kleft:
                    k = kleft + ".ANYINDEX"
                    db_v = {kright: db_v}
                    kleft, _, kright = k.rpartition(".ANYINDEX.")
                deep_update(db_filter, {k: db_v})

        return db_filter

    def _find(self, table, q_filter):

        def recursive_find(key_list, key_next_index, content, operator, target):
            if key_next_index == len(key_list) or content is None:
                try:
                    if operator == "eq":
                        if isinstance(target, list) and not isinstance(content, list):
                            return True if content in target else False
                        return True if content == target else False
                    elif operator in ("neq", "ne"):
                        if isinstance(target, list) and not isinstance(content, list):
                            return True if content not in target else False
                        return True if content != target else False
                    if operator == "gt":
                        return content > target
                    elif operator == "gte":
                        return content >= target
                    elif operator == "lt":
                        return content < target
                    elif operator == "lte":
                        return content <= target
                    elif operator == "cont":
                        return content in target
                    elif operator == "ncont":
                        return content not in target
                    else:
                        raise DbException("Unknown filter operator '{}' in key '{}'".
                                          format(operator, ".".join(key_list)), http_code=HTTPStatus.BAD_REQUEST)
                except TypeError:
                    return False

            elif isinstance(content, dict):
                return recursive_find(key_list, key_next_index+1, content.get(key_list[key_next_index]), operator,
                                      target)
            elif isinstance(content, list):
                look_for_match = True  # when there is a match return immediately
                if (target is None and operator not in ("neq", "ne")) or \
                        (target is not None and operator in ("neq", "ne")):
                    look_for_match = False  # when there is a match return immediately

                for content_item in content:
                    if key_list[key_next_index] == "ANYINDEX" and isinstance(v, dict):
                        for k2, v2 in target.items():
                            k_new_list = k2.split(".")
                            new_operator = "eq"
                            if k_new_list[-1] in ("eq", "ne", "gt", "gte", "lt", "lte", "cont", "ncont", "neq"):
                                new_operator = k_new_list.pop()
                            if not recursive_find(k_new_list, 0, content_item, new_operator, v2):
                                match = False
                                break
                        else:
                            match = True

                    else:
                        match = recursive_find(key_list, key_next_index, content_item, operator, target)
                    if match == look_for_match:
                        return match
                if key_list[key_next_index].isdecimal() and int(key_list[key_next_index]) < len(content):
                    match = recursive_find(key_list, key_next_index+1, content[int(key_list[key_next_index])],
                                           operator, target)
                    if match == look_for_match:
                        return match
                return not look_for_match
            else:  # content is not dict, nor list neither None, so not found
                if operator in ("neq", "ne"):
                    return True if target is None else False
                else:
                    return True if target is None else False

        for i, row in enumerate(self.db.get(table, ())):
            q_filter = q_filter or {}
            for k, v in q_filter.items():
                k_list = k.split(".")
                operator = "eq"
                if k_list[-1] in ("eq", "ne", "gt", "gte", "lt", "lte", "cont", "ncont", "neq"):
                    operator = k_list.pop()
                match = recursive_find(k_list, 0, row, operator, v)
                if not match:
                    break
            else:
                # match
                yield i, row

    def get_list(self, table, q_filter=None):
        """
        Obtain a list of entries matching q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: a list (can be empty) with the found entries. Raises DbException on error
        """
        try:
            result = []
            with self.lock:
                for _, row in self._find(table, self._format_filter(q_filter)):
                    result.append(deepcopy(row))
            return result
        except DbException:
            raise
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def count(self, table, q_filter=None):
        """
        Count the number of entries matching q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: number of entries found (can be zero)
        :raise: DbException on error
        """
        try:
            with self.lock:
                return sum(1 for x in self._find(table, self._format_filter(q_filter)))
        except DbException:
            raise
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def get_one(self, table, q_filter=None, fail_on_empty=True, fail_on_more=True):
        """
        Obtain one entry matching q_filter
        :param table: collection or table
        :param q_filter: Filter
        :param fail_on_empty: If nothing matches filter it returns None unless this flag is set tu True, in which case
        it raises a DbException
        :param fail_on_more: If more than one matches filter it returns one of then unless this flag is set tu True, so
        that it raises a DbException
        :return: The requested element, or None
        """
        try:
            result = None
            with self.lock:
                for _, row in self._find(table, self._format_filter(q_filter)):
                    if not fail_on_more:
                        return deepcopy(row)
                    if result:
                        raise DbException("Found more than one entry with filter='{}'".format(q_filter),
                                          HTTPStatus.CONFLICT.value)
                    result = row
            if not result and fail_on_empty:
                raise DbException("Not found entry with filter='{}'".format(q_filter), HTTPStatus.NOT_FOUND)
            return deepcopy(result)
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def del_list(self, table, q_filter=None):
        """
        Deletes all entries that match q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: Dict with the number of entries deleted
        """
        try:
            id_list = []
            with self.lock:
                for i, _ in self._find(table, self._format_filter(q_filter)):
                    id_list.append(i)
            deleted = len(id_list)
            for i in reversed(id_list):
                del self.db[table][i]
            return {"deleted": deleted}
        except DbException:
            raise
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def del_one(self, table, q_filter=None, fail_on_empty=True):
        """
        Deletes one entry that matches q_filter
        :param table: collection or table
        :param q_filter: Filter
        :param fail_on_empty: If nothing matches filter it returns '0' deleted unless this flag is set tu True, in
        which case it raises a DbException
        :return: Dict with the number of entries deleted
        """
        try:
            with self.lock:
                for i, _ in self._find(table, self._format_filter(q_filter)):
                    break
                else:
                    if fail_on_empty:
                        raise DbException("Not found entry with filter='{}'".format(q_filter), HTTPStatus.NOT_FOUND)
                    return None
                del self.db[table][i]
            return {"deleted": 1}
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def set_one(self, table, q_filter, update_dict, fail_on_empty=True, unset=None, pull=None, push=None):
        """
        Modifies an entry at database
        :param table: collection or table
        :param q_filter: Filter
        :param update_dict: Plain dictionary with the content to be updated. It is a dot separated keys and a value
        :param fail_on_empty: If nothing matches filter it returns None unless this flag is set tu True, in which case
        it raises a DbException
        :param unset: Plain dictionary with the content to be removed if exist. It is a dot separated keys, value is
                      ignored. If not exist, it is ignored
        :param pull: Plain dictionary with the content to be removed from an array. It is a dot separated keys and value
                     if exist in the array is removed. If not exist, it is ignored
        :param push: Plain dictionary with the content to be appended to an array. It is a dot separated keys and value
                     is appended to the end of the array
        :return: Dict with the number of entries modified. None if no matching is found.
        """
        try:
            with self.lock:
                for i, db_item in self._find(table, self._format_filter(q_filter)):
                    break
                else:
                    if fail_on_empty:
                        raise DbException("Not found entry with _id='{}'".format(q_filter), HTTPStatus.NOT_FOUND)
                    return None
                for k, v in update_dict.items():
                    db_nested = db_item
                    k_list = k.split(".")
                    k_nested_prev = k_list[0]
                    for k_nested in k_list[1:]:
                        if isinstance(db_nested[k_nested_prev], dict):
                            if k_nested not in db_nested[k_nested_prev]:
                                db_nested[k_nested_prev][k_nested] = None
                        elif isinstance(db_nested[k_nested_prev], list) and k_nested.isdigit():
                            # extend list with Nones if index greater than list
                            k_nested = int(k_nested)
                            if k_nested >= len(db_nested[k_nested_prev]):
                                db_nested[k_nested_prev] += [None] * (k_nested - len(db_nested[k_nested_prev]) + 1)
                        elif db_nested[k_nested_prev] is None:
                            db_nested[k_nested_prev] = {k_nested: None}
                        else:  # number, string, boolean, ... or list but with not integer key
                            raise DbException("Cannot set '{}' on existing '{}={}'".format(k, k_nested_prev,
                                                                                           db_nested[k_nested_prev]))

                        db_nested = db_nested[k_nested_prev]
                        k_nested_prev = k_nested

                    db_nested[k_nested_prev] = v
                return {"updated": 1}
        except DbException:
            raise
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def replace(self, table, _id, indata, fail_on_empty=True):
        """
        Replace the content of an entry
        :param table: collection or table
        :param _id: internal database id
        :param indata: content to replace
        :param fail_on_empty: If nothing matches filter it returns None unless this flag is set tu True, in which case
        it raises a DbException
        :return: Dict with the number of entries replaced
        """
        try:
            with self.lock:
                for i, _ in self._find(table, self._format_filter({"_id": _id})):
                    break
                else:
                    if fail_on_empty:
                        raise DbException("Not found entry with _id='{}'".format(_id), HTTPStatus.NOT_FOUND)
                    return None
                self.db[table][i] = deepcopy(indata)
            return {"updated": 1}
        except DbException:
            raise
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def create(self, table, indata):
        """
        Add a new entry at database
        :param table: collection or table
        :param indata: content to be added
        :return: database id of the inserted element. Raises a DbException on error
        """
        try:
            id = indata.get("_id")
            if not id:
                id = str(uuid4())
                indata["_id"] = id
            with self.lock:
                if table not in self.db:
                    self.db[table] = []
                self.db[table].append(deepcopy(indata))
            return id
        except Exception as e:  # TODO refine
            raise DbException(str(e))

    def create_list(self, table, indata_list):
        """
        Add a new entry at database
        :param table: collection or table
        :param indata_list: list content to be added
        :return: database ids of the inserted element. Raises a DbException on error
        """
        try:
            _ids = []
            for indata in indata_list:
                _id = indata.get("_id")
                if not _id:
                    _id = str(uuid4())
                    indata["_id"] = _id
                with self.lock:
                    if table not in self.db:
                        self.db[table] = []
                    self.db[table].append(deepcopy(indata))
                _ids.append(_id)
            return _ids
        except Exception as e:  # TODO refine
            raise DbException(str(e))


if __name__ == '__main__':
    # some test code
    db = DbMemory()
    db.create("test", {"_id": 1, "data": 1})
    db.create("test", {"_id": 2, "data": 2})
    db.create("test", {"_id": 3, "data": 3})
    print("must be 3 items:", db.get_list("test"))
    print("must return item 2:", db.get_list("test", {"_id": 2}))
    db.del_one("test", {"_id": 2})
    print("must be emtpy:", db.get_list("test", {"_id": 2}))
