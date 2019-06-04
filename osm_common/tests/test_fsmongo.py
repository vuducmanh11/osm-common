# Copyright 2019 Canonical
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
# contact: eduardo.sousa@canonical.com
##

import logging
import pytest
import tempfile

from pymongo import MongoClient
from gridfs import GridFSBucket

from osm_common.fsbase import FsException
from osm_common.fsmongo import FsMongo

__author__ = "Eduardo Sousa <eduardo.sousa@canonical.com>"


def valid_path():
    return tempfile.gettempdir() + '/'


def invalid_path():
    return '/#tweeter/'


@pytest.fixture(scope="function", params=[True, False])
def fs_mongo(request, monkeypatch):
    def mock_mongoclient_constructor(a, b, c):
        pass

    def mock_mongoclient_getitem(a, b):
        pass

    def mock_gridfs_constructor(a, b):
        pass

    monkeypatch.setattr(MongoClient, '__init__', mock_mongoclient_constructor)
    monkeypatch.setattr(MongoClient, '__getitem__', mock_mongoclient_getitem)
    monkeypatch.setattr(GridFSBucket, '__init__', mock_gridfs_constructor)
    fs = FsMongo(lock=request.param)
    fs.fs_connect({
        'path': valid_path(),
        'host': 'mongo',
        'port': 27017,
        'collection': 'files'})
    return fs


def generic_fs_exception_message(message):
    return "storage exception {}".format(message)


def fs_connect_exception_message(path):
    return "storage exception Invalid configuration param at '[storage]': path '{}' does not exist".format(path)


def file_open_file_not_found_exception(storage):
    f = storage if isinstance(storage, str) else '/'.join(storage)
    return "storage exception File {} does not exist".format(f)


def file_open_io_exception(storage):
    f = storage if isinstance(storage, str) else '/'.join(storage)
    return "storage exception File {} cannot be opened".format(f)


def dir_ls_not_a_directory_exception(storage):
    f = storage if isinstance(storage, str) else '/'.join(storage)
    return "storage exception File {} does not exist".format(f)


def dir_ls_io_exception(storage):
    f = storage if isinstance(storage, str) else '/'.join(storage)
    return "storage exception File {} cannot be opened".format(f)


def file_delete_exception_message(storage):
    return "storage exception File {} does not exist".format(storage)


def test_constructor_without_logger():
    fs = FsMongo()
    assert fs.logger == logging.getLogger('fs')
    assert fs.path is None
    assert fs.client is None
    assert fs.fs is None


def test_constructor_with_logger():
    logger_name = 'fs_mongo'
    fs = FsMongo(logger_name=logger_name)
    assert fs.logger == logging.getLogger(logger_name)
    assert fs.path is None
    assert fs.client is None
    assert fs.fs is None


def test_get_params(fs_mongo, monkeypatch):
    def mock_gridfs_find(self, search_query, **kwargs):
        return []

    monkeypatch.setattr(GridFSBucket, 'find', mock_gridfs_find)
    params = fs_mongo.get_params()
    assert len(params) == 2
    assert "fs" in params
    assert "path" in params
    assert params["fs"] == "mongo"
    assert params["path"] == valid_path()


@pytest.mark.parametrize("config, exp_logger, exp_path", [
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        'fs_mongo', valid_path()
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        'fs_mongo', valid_path()
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path()[:-1],
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        'fs_mongo', valid_path()
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path()[:-1],
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        'fs_mongo', valid_path()
    ),
    (
        {
            'path': valid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        'fs', valid_path()
    ),
    (
        {
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        'fs', valid_path()
    ),
    (
        {
            'path': valid_path()[:-1],
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        'fs', valid_path()
    ),
    (
        {
            'path': valid_path()[:-1],
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        'fs', valid_path()
    )])
def test_fs_connect_with_valid_config(config, exp_logger, exp_path):
    fs = FsMongo()
    fs.fs_connect(config)
    assert fs.logger == logging.getLogger(exp_logger)
    assert fs.path == exp_path
    assert type(fs.client) == MongoClient
    assert type(fs.fs) == GridFSBucket


@pytest.mark.parametrize("config, exp_exception_message", [
    (
        {
            'logger_name': 'fs_mongo',
            'path': invalid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path())
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': invalid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path())
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': invalid_path()[:-1],
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path()[:-1])
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': invalid_path()[:-1],
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path()[:-1])
    ),
    (
        {
            'path': invalid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path())
    ),
    (
        {
            'path': invalid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path())
    ),
    (
        {
            'path': invalid_path()[:-1],
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path()[:-1])
    ),
    (
        {
            'path': invalid_path()[:-1],
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        fs_connect_exception_message(invalid_path()[:-1])
    ),
    (
        {
            'path': '/',
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        generic_fs_exception_message(
            "Invalid configuration param at '[storage]': path '/' is not writable"
        )
    )])
def test_fs_connect_with_invalid_path(config, exp_exception_message):
    fs = FsMongo()
    with pytest.raises(FsException) as excinfo:
        fs.fs_connect(config)
    assert str(excinfo.value) == exp_exception_message


@pytest.mark.parametrize("config, exp_exception_message", [
    (
        {
            'logger_name': 'fs_mongo',
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        "Missing parameter \"path\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        "Missing parameter \"path\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'collection': 'files'
        },
        "Missing parameters: \"uri\" or \"host\" + \"port\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'port': 27017,
            'collection': 'files'
        },
        "Missing parameters: \"uri\" or \"host\" + \"port\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'collection': 'files'
        },
        "Missing parameters: \"uri\" or \"host\" + \"port\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'uri': 'mongo:27017'
        },
        "Missing parameter \"collection\""
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
        },
        "Missing parameter \"collection\""
    )])
def test_fs_connect_with_missing_parameters(config, exp_exception_message):
    fs = FsMongo()
    with pytest.raises(FsException) as excinfo:
        fs.fs_connect(config)
    assert str(excinfo.value) == generic_fs_exception_message(exp_exception_message)


@pytest.mark.parametrize("config, exp_exception_message", [
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        "MongoClient crashed"
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        "MongoClient crashed"
    )])
def test_fs_connect_with_invalid_mongoclient(config, exp_exception_message, monkeypatch):
    def generate_exception(a, b, c=None):
        raise Exception(exp_exception_message)

    monkeypatch.setattr(MongoClient, '__init__', generate_exception)

    fs = FsMongo()
    with pytest.raises(FsException) as excinfo:
        fs.fs_connect(config)
    assert str(excinfo.value) == generic_fs_exception_message(exp_exception_message)


@pytest.mark.parametrize("config, exp_exception_message", [
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        "Collection unavailable"
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        "Collection unavailable"
    )])
def test_fs_connect_with_invalid_mongo_collection(config, exp_exception_message, monkeypatch):
    def mock_mongoclient_constructor(a, b, c=None):
        pass

    def generate_exception(a, b):
        raise Exception(exp_exception_message)

    monkeypatch.setattr(MongoClient, '__init__', mock_mongoclient_constructor)
    monkeypatch.setattr(MongoClient, '__getitem__', generate_exception)

    fs = FsMongo()
    with pytest.raises(FsException) as excinfo:
        fs.fs_connect(config)
    assert str(excinfo.value) == generic_fs_exception_message(exp_exception_message)


@pytest.mark.parametrize("config, exp_exception_message", [
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'uri': 'mongo:27017',
            'collection': 'files'
        },
        "GridFsBucket crashed"
    ),
    (
        {
            'logger_name': 'fs_mongo',
            'path': valid_path(),
            'host': 'mongo',
            'port': 27017,
            'collection': 'files'
        },
        "GridFsBucket crashed"
    )])
def test_fs_connect_with_invalid_gridfsbucket(config, exp_exception_message, monkeypatch):
    def mock_mongoclient_constructor(a, b, c=None):
        pass

    def mock_mongoclient_getitem(a, b):
        pass

    def generate_exception(a, b):
        raise Exception(exp_exception_message)

    monkeypatch.setattr(MongoClient, '__init__', mock_mongoclient_constructor)
    monkeypatch.setattr(MongoClient, '__getitem__', mock_mongoclient_getitem)
    monkeypatch.setattr(GridFSBucket, '__init__', generate_exception)

    fs = FsMongo()
    with pytest.raises(FsException) as excinfo:
        fs.fs_connect(config)
    assert str(excinfo.value) == generic_fs_exception_message(exp_exception_message)


def test_fs_disconnect(fs_mongo):
    fs_mongo.fs_disconnect()
