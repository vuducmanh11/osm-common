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
from http import HTTPStatus
from uuid import uuid4
from copy import deepcopy

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"


class DbMemory(DbBase):

    def __init__(self, logger_name='db'):
        super().__init__(logger_name)
        self.db = {}

    def db_connect(self, config):
        """
        Connect to database
        :param config: Configuration of database
        :return: None or raises DbException on error
        """
        if "logger_name" in config:
            self.logger = logging.getLogger(config["logger_name"])
        self.master_password = config.get("masterpassword")

    @staticmethod
    def _format_filter(q_filter):
        return q_filter    # TODO

    def _find(self, table, q_filter):
        for i, row in enumerate(self.db.get(table, ())):
            match = True
            if q_filter:
                for k, v in q_filter.items():
                    if k not in row or v != row[k]:
                        match = False
            if match:
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
            for _, row in self._find(table, self._format_filter(q_filter)):
                result.append(deepcopy(row))
            return result
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
            if table not in self.db:
                self.db[table] = []
            self.db[table].append(deepcopy(indata))
            return id
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
