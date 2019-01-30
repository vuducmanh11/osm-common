# Copyright 2018 Whitestack, LLC
# Copyright 2018 Telefonica S.A.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# For those usages not covered by the Apache License, Version 2.0 please
# contact: esousa@whitestack.com or alfonso.tiernosepulveda@telefonica.com
##

import http
import pytest
import unittest
from osm_common.dbbase import DbBase, DbException, deep_update
from os import urandom
from http import HTTPStatus


def exception_message(message):
    return "database exception " + message


@pytest.fixture
def db_base():
    return DbBase()


def test_constructor():
    db_base = DbBase()
    assert db_base is not None
    assert isinstance(db_base, DbBase)


def test_db_connect(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.db_connect(None)
    assert str(excinfo.value).startswith(exception_message("Method 'db_connect' not implemented"))


def test_db_disconnect(db_base):
    db_base.db_disconnect()


def test_get_list(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.get_list(None, None)
    assert str(excinfo.value).startswith(exception_message("Method 'get_list' not implemented"))
    assert excinfo.value.http_code == http.HTTPStatus.NOT_FOUND


def test_get_one(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.get_one(None, None, None, None)
    assert str(excinfo.value).startswith(exception_message("Method 'get_one' not implemented"))
    assert excinfo.value.http_code == http.HTTPStatus.NOT_FOUND


def test_create(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.create(None, None)
    assert str(excinfo.value).startswith(exception_message("Method 'create' not implemented"))
    assert excinfo.value.http_code == http.HTTPStatus.NOT_FOUND


def test_del_list(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.del_list(None, None)
    assert str(excinfo.value).startswith(exception_message("Method 'del_list' not implemented"))
    assert excinfo.value.http_code == http.HTTPStatus.NOT_FOUND


def test_del_one(db_base):
    with pytest.raises(DbException) as excinfo:
        db_base.del_one(None, None, None)
    assert str(excinfo.value).startswith(exception_message("Method 'del_one' not implemented"))
    assert excinfo.value.http_code == http.HTTPStatus.NOT_FOUND


class TestEncryption(unittest.TestCase):
    def setUp(self):
        master_key = "Setting a long master key with numbers 123 and capitals AGHBNHD and symbols %&8)!'"
        db_base1 = DbBase()
        db_base2 = DbBase()
        db_base3 = DbBase()
        # set self.secret_key obtained when connect
        db_base1.set_secret_key(master_key, replace=True)
        db_base1.set_secret_key(urandom(32))
        db_base2.set_secret_key(None, replace=True)
        db_base2.set_secret_key(urandom(30))
        db_base3.set_secret_key(master_key)
        self.db_bases = [db_base1, db_base2, db_base3]

    def test_encrypt_decrypt(self):
        TEST = (
            ("plain text 1 ! ", None),
            ("plain text 2 with salt ! ", "1afd5d1a-4a7e-4d9c-8c65-251290183106"),
            ("plain text 3 with usalt ! ", u"1afd5d1a-4a7e-4d9c-8c65-251290183106"),
            (u"plain unicode 4 ! ", None),
            (u"plain unicode 5 with salt ! ", "1a000d1a-4a7e-4d9c-8c65-251290183106"),
            (u"plain unicode 6 with usalt ! ", u"1abcdd1a-4a7e-4d9c-8c65-251290183106"),
        )
        for db_base in self.db_bases:
            for value, salt in TEST:
                # no encryption
                encrypted = db_base.encrypt(value, schema_version='1.0', salt=salt)
                self.assertEqual(encrypted, value, "value '{}' has been encrypted".format(value))
                decrypted = db_base.decrypt(encrypted, schema_version='1.0', salt=salt)
                self.assertEqual(decrypted, value, "value '{}' has been decrypted".format(value))

                # encrypt/decrypt
                encrypted = db_base.encrypt(value, schema_version='1.1', salt=salt)
                self.assertNotEqual(encrypted, value, "value '{}' has not been encrypted".format(value))
                self.assertIsInstance(encrypted, str, "Encrypted is not ascii text")
                decrypted = db_base.decrypt(encrypted, schema_version='1.1', salt=salt)
                self.assertEqual(decrypted, value, "value is not equal after encryption/decryption")

    def test_encrypt_decrypt_salt(self):
        value = "value to be encrypted!"
        encrypted = []
        for db_base in self.db_bases:
            for salt in (None, "salt 1", "1afd5d1a-4a7e-4d9c-8c65-251290183106"):
                # encrypt/decrypt
                encrypted.append(db_base.encrypt(value, schema_version='1.1', salt=salt))
                self.assertNotEqual(encrypted[-1], value, "value '{}' has not been encrypted".format(value))
                self.assertIsInstance(encrypted[-1], str, "Encrypted is not ascii text")
                decrypted = db_base.decrypt(encrypted[-1], schema_version='1.1', salt=salt)
                self.assertEqual(decrypted, value, "value is not equal after encryption/decryption")
        for i in range(0, len(encrypted)):
            for j in range(i+1, len(encrypted)):
                self.assertNotEqual(encrypted[i], encrypted[j],
                                    "encryption with different salt must contain different result")
        # decrypt with a different master key
        try:
            decrypted = self.db_bases[-1].decrypt(encrypted[0], schema_version='1.1', salt=None)
            self.assertNotEqual(encrypted[0], decrypted, "Decryption with different KEY must generate different result")
        except DbException as e:
            self.assertEqual(e.http_code, HTTPStatus.INTERNAL_SERVER_ERROR,
                             "Decryption with different KEY does not provide expected http_code")


class TestDeepUpdate(unittest.TestCase):
    def test_update_dict(self):
        # Original, patch, expected result
        TEST = (
            ({"a": "b"}, {"a": "c"}, {"a": "c"}),
            ({"a": "b"}, {"b": "c"}, {"a": "b", "b": "c"}),
            ({"a": "b"}, {"a": None}, {}),
            ({"a": "b", "b": "c"}, {"a": None}, {"b": "c"}),
            ({"a": ["b"]}, {"a": "c"}, {"a": "c"}),
            ({"a": "c"}, {"a": ["b"]}, {"a": ["b"]}),
            ({"a": {"b": "c"}}, {"a": {"b": "d", "c": None}}, {"a": {"b": "d"}}),
            ({"a": [{"b": "c"}]}, {"a": [1]}, {"a": [1]}),
            ({1: ["a", "b"]}, {1: ["c", "d"]}, {1: ["c", "d"]}),
            ({1: {"a": "b"}}, {1: ["c"]}, {1: ["c"]}),
            ({1: {"a": "foo"}}, {1: None}, {}),
            ({1: {"a": "foo"}}, {1: "bar"}, {1: "bar"}),
            ({"e": None}, {"a": 1}, {"e": None, "a": 1}),
            ({1: [1, 2]}, {1: {"a": "b", "c": None}}, {1: {"a": "b"}}),
            ({}, {"a": {"bb": {"ccc": None}}}, {"a": {"bb": {}}}),
        )
        for t in TEST:
            deep_update(t[0], t[1])
            self.assertEqual(t[0], t[2])
        # test deepcopy is done. So that original dictionary does not reference the pach
        test_original = {1: {"a": "b"}}
        test_patch = {1: {"c": {"d": "e"}}}
        test_result = {1: {"a": "b", "c": {"d": "e"}}}
        deep_update(test_original, test_patch)
        self.assertEqual(test_original, test_result)
        test_patch[1]["c"]["f"] = "edition of patch, must not modify original"
        self.assertEqual(test_original, test_result)

    def test_update_array(self):
        # This TEST contains a list with the the Original, patch, and expected result
        TEST = (
            # delete all instances of "a"/"d"
            ({"A": ["a", "b", "a"]}, {"A": {"$a": None}}, {"A": ["b"]}),
            ({"A": ["a", "b", "a"]}, {"A": {"$d": None}}, {"A": ["a", "b", "a"]}),
            # delete and insert at 0
            ({"A": ["a", "b", "c"]}, {"A": {"$b": None, "$+[0]": "b"}}, {"A": ["b", "a", "c"]}),
            # delete and edit
            ({"A": ["a", "b", "a"]}, {"A": {"$a": None, "$[1]": {"c": "d"}}}, {"A": [{"c": "d"}]}),
            # insert if not exist
            ({"A": ["a", "b", "c"]}, {"A": {"$+b": "b"}}, {"A": ["a", "b", "c"]}),
            ({"A": ["a", "b", "c"]}, {"A": {"$+d": "f"}}, {"A": ["a", "b", "c", "f"]}),
            # edit by filter
            ({"A": ["a", "b", "a"]}, {"A": {"$b": {"c": "d"}}}, {"A": ["a", {"c": "d"}, "a"]}),
            ({"A": ["a", "b", "a"]}, {"A": {"$b": None, "$+[0]": "b", "$+": "c"}}, {"A": ["b", "a", "a", "c"]}),
            ({"A": ["a", "b", "a"]}, {"A": {"$c": None}}, {"A": ["a", "b", "a"]}),
            # index deletion out of range
            ({"A": ["a", "b", "a"]}, {"A": {"$[5]": None}}, {"A": ["a", "b", "a"]}),
            # nested array->dict
            ({"A": ["a", "b", {"id": "1", "c": {"d": 2}}]}, {"A": {"$id: '1'": {"h": None, "c": {"d": "e", "f": "g"}}}},
             {"A": ["a", "b", {"id": "1", "c": {"d": "e", "f": "g"}}]}),
            ({"A": [{"id": 1, "c": {"d": 2}}, {"id": 1, "c": {"f": []}}]},
             {"A": {"$id: 1": {"h": None, "c": {"d": "e", "f": "g"}}}},
             {"A": [{"id": 1, "c": {"d": "e", "f": "g"}}, {"id": 1, "c": {"d": "e", "f": "g"}}]}),
            # nested array->array
            ({"A": ["a", "b", ["a", "b"]]}, {"A": {"$b": None, "$[2]": {"$b": {}, "$+": "c"}}},
             {"A": ["a", ["a", {}, "c"]]}),
            # types str and int different, so not found
            ({"A": ["a", {"id": "1", "c": "d"}]}, {"A": {"$id: 1": {"c": "e"}}}, {"A": ["a", {"id": "1", "c": "d"}]}),

        )
        for t in TEST:
            print(t)
            deep_update(t[0], t[1])
            self.assertEqual(t[0], t[2])

    def test_update_badformat(self):
        # This TEST contains original, incorrect patch and #TODO text that must be present
        TEST = (
            # conflict, index 0 is edited twice
            ({"A": ["a", "b", "a"]}, {"A": {"$a": None, "$[0]": {"c": "d"}}}),
            # conflict, two insertions at same index
            ({"A": ["a", "b", "a"]}, {"A": {"$[1]": "c", "$[-2]": "d"}}),
            ({"A": ["a", "b", "a"]}, {"A": {"$[1]": "c", "$[+1]": "d"}}),
            # bad format keys with and without $
            ({"A": ["a", "b", "a"]}, {"A": {"$b": {"c": "d"}, "c": 3}}),
            # bad format empty $ and yaml incorrect
            ({"A": ["a", "b", "a"]}, {"A": {"$": 3}}),
            ({"A": ["a", "b", "a"]}, {"A": {"$a: b: c": 3}}),
            ({"A": ["a", "b", "a"]}, {"A": {"$a: b, c: d": 3}}),
            # insertion of None
            ({"A": ["a", "b", "a"]}, {"A": {"$+": None}}),
            # Not found, insertion of None
            ({"A": ["a", "b", "a"]}, {"A": {"$+c": None}}),
            # index edition out of range
            ({"A": ["a", "b", "a"]}, {"A": {"$[5]": 6}}),
            # conflict, two editions on index 2
            ({"A": ["a", {"id": "1", "c": "d"}]}, {"A": {"$id: '1'": {"c": "e"}, "$c: d": {"c": "f"}}}),
        )
        for t in TEST:
            print(t)
            self.assertRaises(DbException, deep_update, t[0], t[1])
            try:
                deep_update(t[0], t[1])
            except DbException as e:
                print(e)


if __name__ == '__main__':
    unittest.main()
