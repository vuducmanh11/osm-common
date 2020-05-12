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

import yaml
import logging
import re
from http import HTTPStatus
from copy import deepcopy
from Crypto.Cipher import AES
from base64 import b64decode, b64encode
from osm_common.common_utils import FakeLock
from threading import Lock

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"


class DbException(Exception):

    def __init__(self, message, http_code=HTTPStatus.NOT_FOUND):
        self.http_code = http_code
        Exception.__init__(self, "database exception " + str(message))


class DbBase(object):

    def __init__(self, logger_name='db', lock=False):
        """
        Constructor of dbBase
        :param logger_name: logging name
        :param lock: Used to protect simultaneous access to the same instance class by several threads:
            False, None: Do not protect, this object will only be accessed by one thread
            True: This object needs to be protected by several threads accessing.
            Lock object. Use thi Lock for the threads access protection
        """
        self.logger = logging.getLogger(logger_name)
        self.secret_key = None  # 32 bytes length array used for encrypt/decrypt
        if not lock:
            self.lock = FakeLock()
        elif lock is True:
            self.lock = Lock()
        elif isinstance(lock, Lock):
            self.lock = lock
        else:
            raise ValueError("lock parameter must be a Lock classclass or boolean")

    def db_connect(self, config, target_version=None):
        """
        Connect to database
        :param config: Configuration of database. Contains among others:
            host:   database host (mandatory)
            port:   database port (mandatory)
            name:   database name (mandatory)
            user:   database username
            password:   database password
            commonkey: common OSM key used for sensible information encryption
            materpassword: same as commonkey, for backward compatibility. Deprecated, to be removed in the future
        :param target_version: if provided it checks if database contains required version, raising exception otherwise.
        :return: None or raises DbException on error
        """
        raise DbException("Method 'db_connect' not implemented")

    def db_disconnect(self):
        """
        Disconnect from database
        :return: None
        """
        pass

    def get_list(self, table, q_filter=None):
        """
        Obtain a list of entries matching q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: a list (can be empty) with the found entries. Raises DbException on error
        """
        raise DbException("Method 'get_list' not implemented")

    def count(self, table, q_filter=None):
        """
        Count the number of entries matching q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: number of entries found (can be zero)
        :raise: DbException on error
        """
        raise DbException("Method 'count' not implemented")

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
        raise DbException("Method 'get_one' not implemented")

    def del_list(self, table, q_filter=None):
        """
        Deletes all entries that match q_filter
        :param table: collection or table
        :param q_filter: Filter
        :return: Dict with the number of entries deleted
        """
        raise DbException("Method 'del_list' not implemented")

    def del_one(self, table, q_filter=None, fail_on_empty=True):
        """
        Deletes one entry that matches q_filter
        :param table: collection or table
        :param q_filter: Filter
        :param fail_on_empty: If nothing matches filter it returns '0' deleted unless this flag is set tu True, in
        which case it raises a DbException
        :return: Dict with the number of entries deleted
        """
        raise DbException("Method 'del_one' not implemented")

    def create(self, table, indata):
        """
        Add a new entry at database
        :param table: collection or table
        :param indata: content to be added
        :return: database '_id' of the inserted element. Raises a DbException on error
        """
        raise DbException("Method 'create' not implemented")

    def create_list(self, table, indata_list):
        """
        Add several entries at once
        :param table: collection or table
        :param indata_list: list of elements to insert. Each element must be a dictionary.
            An '_id' key based on random uuid is added at each element if missing
        :return: list of inserted '_id's. Exception on error
        """
        raise DbException("Method 'create_list' not implemented")

    def set_one(self, table, q_filter, update_dict, fail_on_empty=True, unset=None, pull=None, push=None,
                push_list=None):
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
        :param push_list: Same as push but values are arrays where each item is and appended instead of appending the
                          whole array
        :return: Dict with the number of entries modified. None if no matching is found.
        """
        raise DbException("Method 'set_one' not implemented")

    def set_list(self, table, q_filter, update_dict, unset=None, pull=None, push=None, push_list=None):
        """
        Modifies al matching entries at database
        :param table: collection or table
        :param q_filter: Filter
        :param update_dict: Plain dictionary with the content to be updated. It is a dot separated keys and a value
        :param unset: Plain dictionary with the content to be removed if exist. It is a dot separated keys, value is
                      ignored. If not exist, it is ignored
        :param pull: Plain dictionary with the content to be removed from an array. It is a dot separated keys and value
                     if exist in the array is removed. If not exist, it is ignored
        :param push: Plain dictionary with the content to be appended to an array. It is a dot separated keys and value
                     is appended to the end of the array
        :param push_list: Same as push but values are arrays where each item is and appended instead of appending the
                          whole array
        :return: Dict with the number of entries modified
        """
        raise DbException("Method 'set_list' not implemented")

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
        raise DbException("Method 'replace' not implemented")

    def _join_secret_key(self, update_key):
        """
        Returns a xor byte combination of the internal secret_key and the provided update_key.
        It does not modify the internal secret_key. Used for adding salt, join keys, etc.
        :param update_key: Can be a string, byte or None. Recommended a long one (e.g. 32 byte length)
        :return: joined key in bytes with a 32 bytes length. Can be None if both internal secret_key and update_key
                 are None
        """
        if not update_key:
            return self.secret_key
        elif isinstance(update_key, str):
            update_key_bytes = update_key.encode()
        else:
            update_key_bytes = update_key

        new_secret_key = bytearray(self.secret_key) if self.secret_key else bytearray(32)
        for i, b in enumerate(update_key_bytes):
            new_secret_key[i % 32] ^= b
        return bytes(new_secret_key)

    def set_secret_key(self, new_secret_key, replace=False):
        """
        Updates internal secret_key used for encryption, with a byte xor
        :param new_secret_key: string or byte array. It is recommended a 32 byte length
        :param replace: if True, old value of internal secret_key is ignored and replaced. If false, a byte xor is used
        :return: None
        """
        if replace:
            self.secret_key = None
        self.secret_key = self._join_secret_key(new_secret_key)

    def get_secret_key(self):
        """
        Get the database secret key in case it is not done when "connect" is called. It can happens when database is
        empty after an initial install. It should skip if secret is already obtained.
        """
        pass

    def encrypt(self, value, schema_version=None, salt=None):
        """
        Encrypt a value
        :param value: value to be encrypted. It is string/unicode
        :param schema_version: used for version control. If None or '1.0' no encryption is done.
               If '1.1' symmetric AES encryption is done
        :param salt: optional salt to be used. Must be str
        :return: Encrypted content of value
        """
        self.get_secret_key()
        if not self.secret_key or not schema_version or schema_version == '1.0':
            return value
        else:
            secret_key = self._join_secret_key(salt)
            cipher = AES.new(secret_key)
            padded_private_msg = value + ('\0' * ((16-len(value)) % 16))
            encrypted_msg = cipher.encrypt(padded_private_msg)
            encoded_encrypted_msg = b64encode(encrypted_msg)
            return encoded_encrypted_msg.decode("ascii")

    def decrypt(self, value, schema_version=None, salt=None):
        """
        Decrypt an encrypted value
        :param value: value to be decrypted. It is a base64 string
        :param schema_version: used for known encryption method used. If None or '1.0' no encryption has been done.
               If '1.1' symmetric AES encryption has been done
        :param salt: optional salt to be used
        :return: Plain content of value
        """
        self.get_secret_key()
        if not self.secret_key or not schema_version or schema_version == '1.0':
            return value
        else:
            secret_key = self._join_secret_key(salt)
            encrypted_msg = b64decode(value)
            cipher = AES.new(secret_key)
            decrypted_msg = cipher.decrypt(encrypted_msg)
            try:
                unpadded_private_msg = decrypted_msg.decode().rstrip('\0')
            except UnicodeDecodeError:
                raise DbException("Cannot decrypt information. Are you using same COMMONKEY in all OSM components?",
                                  http_code=HTTPStatus.INTERNAL_SERVER_ERROR)
            return unpadded_private_msg

    def encrypt_decrypt_fields(self, item, action, fields=None, flags=None, schema_version=None, salt=None):
        if not fields:
            return
        self.get_secret_key()
        actions = ['encrypt', 'decrypt']
        if action.lower() not in actions:
            raise DbException("Unknown action ({}): Must be one of {}".format(action, actions),
                              http_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        method = self.encrypt if action.lower() == 'encrypt' else self.decrypt
        if flags is None:
            flags = re.I

        def process(_item):
            if isinstance(_item, list):
                for elem in _item:
                    process(elem)
            elif isinstance(_item, dict):
                for key, val in _item.items():
                    if isinstance(val, str):
                        if any(re.search(f, key, flags) for f in fields):
                            _item[key] = method(val, schema_version, salt)
                    else:
                        process(val)
        process(item)


def deep_update_rfc7396(dict_to_change, dict_reference, key_list=None):
    """
    Modifies one dictionary with the information of the other following https://tools.ietf.org/html/rfc7396
    Basically is a recursive python 'dict_to_change.update(dict_reference)', but a value of None is used to delete.
    It implements an extra feature that allows modifying an array. RFC7396 only allows replacing the entire array.
    For that, dict_reference should contains a dict with keys starting by "$" with the following meaning:
        $[index]    <index> is an integer for targeting a concrete index from dict_to_change array. If the value is None
                    the element of the array is deleted, otherwise it is edited.
        $+[index]   The value is inserted at this <index>. A value of None has not sense and an exception is raised.
        $+          The value is appended at the end. A value of None has not sense and an exception is raised.
        $val        It looks for all the items in the array dict_to_change equal to <val>. <val> is evaluated as yaml,
                    that is, numbers are taken as type int, true/false as boolean, etc. Use quotes to force string.
                    Nothing happens if no match is found. If the value is None the matched elements are deleted.
        $key: val   In case a dictionary is passed in yaml format, if looks for all items in the array dict_to_change
                    that are dictionaries and contains this <key> equal to <val>. Several keys can be used by yaml
                    format '{key: val, key: val, ...}'; and all of them must match. Nothing happens if no match is
                    found. If value is None the matched items are deleted, otherwise they are edited.
        $+val       If no match if found (see '$val'), the value is appended to the array. If any match is found nothing
                    is changed. A value of None has not sense.
        $+key: val  If no match if found (see '$key: val'), the value is appended to the array. If any match is found
                    nothing is changed. A value of None has not sense.
    If there are several editions, insertions and deletions; editions and deletions are done first in reverse index
        order; then insertions also in reverse index order; and finally appends in any order. So indexes used at
        insertions must take into account the deleted items.
    :param dict_to_change:  Target dictionary to be changed.
    :param dict_reference: Dictionary that contains changes to be applied.
    :param key_list: This is used internally for recursive calls. Do not fill this parameter.
    :return: none or raises and exception only at array modification when there is a bad format or conflict.
    """
    def _deep_update_array(array_to_change, _dict_reference, _key_list):
        to_append = {}
        to_insert_at_index = {}
        values_to_edit_delete = {}
        indexes_to_edit_delete = []
        array_edition = None
        _key_list.append("")
        for k in _dict_reference:
            _key_list[-1] = str(k)
            if not isinstance(k, str) or not k.startswith("$"):
                if array_edition is True:
                    raise DbException("Found array edition (keys starting with '$') and pure dictionary edition in the"
                                      " same dict at '{}'".format(":".join(_key_list[:-1])))
                array_edition = False
                continue
            else:
                if array_edition is False:
                    raise DbException("Found array edition (keys starting with '$') and pure dictionary edition in the"
                                      " same dict at '{}'".format(":".join(_key_list[:-1])))
                array_edition = True
            insert = False
            indexes = []  # indexes to edit or insert
            kitem = k[1:]
            if kitem.startswith('+'):
                insert = True
                kitem = kitem[1:]
                if _dict_reference[k] is None:
                    raise DbException("A value of None has not sense for insertions at '{}'".format(
                        ":".join(_key_list)))

            if kitem.startswith('[') and kitem.endswith(']'):
                try:
                    index = int(kitem[1:-1])
                    if index < 0:
                        index += len(array_to_change)
                    if index < 0:
                        index = 0  # skip outside index edition
                    indexes.append(index)
                except Exception:
                    raise DbException("Wrong format at '{}'. Expecting integer index inside quotes".format(
                        ":".join(_key_list)))
            elif kitem:
                # match_found_skip = False
                try:
                    filter_in = yaml.safe_load(kitem)
                except Exception:
                    raise DbException("Wrong format at '{}'. Expecting '$<yaml-format>'".format(":".join(_key_list)))
                if isinstance(filter_in, dict):
                    for index, item in enumerate(array_to_change):
                        for filter_k, filter_v in filter_in.items():
                            if not isinstance(item, dict) or filter_k not in item or item[filter_k] != filter_v:
                                break
                        else:  # match found
                            if insert:
                                # match_found_skip = True
                                insert = False
                                break
                            else:
                                indexes.append(index)
                else:
                    index = 0
                    try:
                        while True:  # if not match a ValueError exception will be raise
                            index = array_to_change.index(filter_in, index)
                            if insert:
                                # match_found_skip = True
                                insert = False
                                break
                            indexes.append(index)
                            index += 1
                    except ValueError:
                        pass

                # if match_found_skip:
                #     continue
            elif not insert:
                raise DbException("Wrong format at '{}'. Expecting '$+', '$[<index]' or '$[<filter>]'".format(
                    ":".join(_key_list)))
            for index in indexes:
                if insert:
                    if index in to_insert_at_index and to_insert_at_index[index] != _dict_reference[k]:
                        # Several different insertions on the same item of the array
                        raise DbException("Conflict at '{}'. Several insertions on same array index {}".format(
                            ":".join(_key_list), index))
                    to_insert_at_index[index] = _dict_reference[k]
                else:
                    if index in indexes_to_edit_delete and values_to_edit_delete[index] != _dict_reference[k]:
                        # Several different editions on the same item of the array
                        raise DbException("Conflict at '{}'. Several editions on array index {}".format(
                            ":".join(_key_list), index))
                    indexes_to_edit_delete.append(index)
                    values_to_edit_delete[index] = _dict_reference[k]
            if not indexes:
                if insert:
                    to_append[k] = _dict_reference[k]
                # elif _dict_reference[k] is not None:
                #     raise DbException("Not found any match to edit in the array, or wrong format at '{}'".format(
                #         ":".join(_key_list)))

        # edition/deletion is done before insertion
        indexes_to_edit_delete.sort(reverse=True)
        for index in indexes_to_edit_delete:
            _key_list[-1] = str(index)
            try:
                if values_to_edit_delete[index] is None:  # None->Anything
                    try:
                        del (array_to_change[index])
                    except IndexError:
                        pass  # it is not consider an error if this index does not exist
                elif not isinstance(values_to_edit_delete[index], dict):  # NotDict->Anything
                    array_to_change[index] = deepcopy(values_to_edit_delete[index])
                elif isinstance(array_to_change[index], dict):  # Dict->Dict
                    deep_update_rfc7396(array_to_change[index], values_to_edit_delete[index], _key_list)
                else:  # Dict->NotDict
                    if isinstance(array_to_change[index], list):  # Dict->List. Check extra array edition
                        if _deep_update_array(array_to_change[index], values_to_edit_delete[index], _key_list):
                            continue
                    array_to_change[index] = deepcopy(values_to_edit_delete[index])
                    # calling deep_update_rfc7396 to delete the None values
                    deep_update_rfc7396(array_to_change[index], values_to_edit_delete[index], _key_list)
            except IndexError:
                raise DbException("Array edition index out of range at '{}'".format(":".join(_key_list)))

        # insertion with indexes
        to_insert_indexes = list(to_insert_at_index.keys())
        to_insert_indexes.sort(reverse=True)
        for index in to_insert_indexes:
            array_to_change.insert(index, to_insert_at_index[index])

        # append
        for k, insert_value in to_append.items():
            _key_list[-1] = str(k)
            insert_value_copy = deepcopy(insert_value)
            if isinstance(insert_value_copy, dict):
                # calling deep_update_rfc7396 to delete the None values
                deep_update_rfc7396(insert_value_copy, insert_value, _key_list)
            array_to_change.append(insert_value_copy)

        _key_list.pop()
        if array_edition:
            return True
        return False

    if key_list is None:
        key_list = []
    key_list.append("")
    for k in dict_reference:
        key_list[-1] = str(k)
        if dict_reference[k] is None:   # None->Anything
            if k in dict_to_change:
                del dict_to_change[k]
        elif not isinstance(dict_reference[k], dict):  # NotDict->Anything
            dict_to_change[k] = deepcopy(dict_reference[k])
        elif k not in dict_to_change:  # Dict->Empty
            dict_to_change[k] = deepcopy(dict_reference[k])
            # calling deep_update_rfc7396 to delete the None values
            deep_update_rfc7396(dict_to_change[k], dict_reference[k], key_list)
        elif isinstance(dict_to_change[k], dict):  # Dict->Dict
            deep_update_rfc7396(dict_to_change[k], dict_reference[k], key_list)
        else:       # Dict->NotDict
            if isinstance(dict_to_change[k], list):  # Dict->List. Check extra array edition
                if _deep_update_array(dict_to_change[k], dict_reference[k], key_list):
                    continue
            dict_to_change[k] = deepcopy(dict_reference[k])
            # calling deep_update_rfc7396 to delete the None values
            deep_update_rfc7396(dict_to_change[k], dict_reference[k], key_list)
    key_list.pop()


def deep_update(dict_to_change, dict_reference):
    """ Maintained for backward compatibility. Use deep_update_rfc7396 instead"""
    return deep_update_rfc7396(dict_to_change, dict_reference)
