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
import logging
import pytest
import tempfile
import shutil
import uuid
import os
import yaml
import time
import threading

from unittest.mock import MagicMock
from osm_common.msgbase import MsgException
from osm_common.msglocal import MsgLocal

__author__ = "Eduardo Sousa <eduardosousa@av.it.pt>"


def valid_path():
    return tempfile.gettempdir() + '/'


def invalid_path():
    return '/#tweeter/'


@pytest.fixture(scope="function", params=[True, False])
def msg_local(request):
    msg = MsgLocal(lock=request.param)
    yield msg

    msg.disconnect()
    if msg.path and msg.path != invalid_path() and msg.path != valid_path():
        shutil.rmtree(msg.path)


@pytest.fixture(scope="function", params=[True, False])
def msg_local_config(request):
    msg = MsgLocal(lock=request.param)
    msg.connect({"path": valid_path() + str(uuid.uuid4())})
    yield msg

    msg.disconnect()
    if msg.path != invalid_path():
        shutil.rmtree(msg.path)


@pytest.fixture(scope="function", params=[True, False])
def msg_local_with_data(request):
    msg = MsgLocal(lock=request.param)
    msg.connect({"path": valid_path() + str(uuid.uuid4())})

    msg.write("topic1", "key1", "msg1")
    msg.write("topic1", "key2", "msg1")
    msg.write("topic2", "key1", "msg1")
    msg.write("topic2", "key2", "msg1")
    msg.write("topic1", "key1", "msg2")
    msg.write("topic1", "key2", "msg2")
    msg.write("topic2", "key1", "msg2")
    msg.write("topic2", "key2", "msg2")
    yield msg

    msg.disconnect()
    if msg.path != invalid_path():
        shutil.rmtree(msg.path)


def empty_exception_message():
    return "messaging exception "


def test_constructor():
    msg = MsgLocal()
    assert msg.logger == logging.getLogger('msg')
    assert msg.path is None
    assert len(msg.files_read) == 0
    assert len(msg.files_write) == 0
    assert len(msg.buffer) == 0


def test_constructor_with_logger():
    logger_name = 'msg_local'
    msg = MsgLocal(logger_name=logger_name)
    assert msg.logger == logging.getLogger(logger_name)
    assert msg.path is None
    assert len(msg.files_read) == 0
    assert len(msg.files_write) == 0
    assert len(msg.buffer) == 0


@pytest.mark.parametrize("config, logger_name, path", [
    ({"logger_name": "msg_local", "path": valid_path()}, "msg_local", valid_path()),
    ({"logger_name": "msg_local", "path": valid_path()[:-1]}, "msg_local", valid_path()),
    ({"logger_name": "msg_local", "path": valid_path() + "test_it/"}, "msg_local", valid_path() + "test_it/"),
    ({"logger_name": "msg_local", "path": valid_path() + "test_it"}, "msg_local", valid_path() + "test_it/"),
    ({"path": valid_path()}, "msg", valid_path()),
    ({"path": valid_path()[:-1]}, "msg", valid_path()),
    ({"path": valid_path() + "test_it/"}, "msg", valid_path() + "test_it/"),
    ({"path": valid_path() + "test_it"}, "msg", valid_path() + "test_it/")])
def test_connect(msg_local, config, logger_name, path):
    msg_local.connect(config)
    assert msg_local.logger == logging.getLogger(logger_name)
    assert msg_local.path == path
    assert len(msg_local.files_read) == 0
    assert len(msg_local.files_write) == 0
    assert len(msg_local.buffer) == 0


@pytest.mark.parametrize("config", [
    ({"logger_name": "msg_local", "path": invalid_path()}),
    ({"path": invalid_path()})])
def test_connect_with_exception(msg_local, config):
    with pytest.raises(MsgException) as excinfo:
        msg_local.connect(config)
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR


def test_disconnect(msg_local_config):
    files_read = msg_local_config.files_read.copy()
    files_write = msg_local_config.files_write.copy()
    msg_local_config.disconnect()
    for f in files_read.values():
        assert f.closed
    for f in files_write.values():
        assert f.closed


def test_disconnect_with_read(msg_local_config):
    msg_local_config.read('topic1', blocks=False)
    msg_local_config.read('topic2', blocks=False)
    files_read = msg_local_config.files_read.copy()
    files_write = msg_local_config.files_write.copy()
    msg_local_config.disconnect()
    for f in files_read.values():
        assert f.closed
    for f in files_write.values():
        assert f.closed


def test_disconnect_with_write(msg_local_with_data):
    files_read = msg_local_with_data.files_read.copy()
    files_write = msg_local_with_data.files_write.copy()
    msg_local_with_data.disconnect()

    for f in files_read.values():
        assert f.closed
    
    for f in files_write.values():
        assert f.closed


def test_disconnect_with_read_and_write(msg_local_with_data):
    msg_local_with_data.read('topic1', blocks=False)
    msg_local_with_data.read('topic2', blocks=False)
    files_read = msg_local_with_data.files_read.copy()
    files_write = msg_local_with_data.files_write.copy()

    msg_local_with_data.disconnect()
    for f in files_read.values():
        assert f.closed
    for f in files_write.values():
        assert f.closed


@pytest.mark.parametrize("topic, key, msg", [
    ("test_topic", "test_key", "test_msg"),
    ("test", "test_key", "test_msg"),
    ("test_topic", "test", "test_msg"),
    ("test_topic", "test_key", "test"),
    ("test_topic", "test_list", ["a", "b", "c"]),
    ("test_topic", "test_tuple", ("c", "b", "a")),
    ("test_topic", "test_dict", {"a": 1, "b": 2, "c": 3}),
    ("test_topic", "test_number", 123),
    ("test_topic", "test_float", 1.23),
    ("test_topic", "test_boolean", True),
    ("test_topic", "test_none", None)])
def test_write(msg_local_config, topic, key, msg):
    file_path = msg_local_config.path + topic
    msg_local_config.write(topic, key, msg)
    assert os.path.exists(file_path)

    with open(file_path, 'r') as stream:
        assert yaml.safe_load(stream) == {key: msg if not isinstance(msg, tuple) else list(msg)}


@pytest.mark.parametrize("topic, key, msg, times", [
    ("test_topic", "test_key", "test_msg", 2),
    ("test", "test_key", "test_msg", 3),
    ("test_topic", "test", "test_msg", 4),
    ("test_topic", "test_key", "test", 2),
    ("test_topic", "test_list", ["a", "b", "c"], 3),
    ("test_topic", "test_tuple", ("c", "b", "a"), 4),
    ("test_topic", "test_dict", {"a": 1, "b": 2, "c": 3}, 2),
    ("test_topic", "test_number", 123, 3),
    ("test_topic", "test_float", 1.23, 4),
    ("test_topic", "test_boolean", True, 2),
    ("test_topic", "test_none", None, 3)])
def test_write_with_multiple_calls(msg_local_config, topic, key, msg, times):
    file_path = msg_local_config.path + topic
    
    for _ in range(times):
        msg_local_config.write(topic, key, msg)
    assert os.path.exists(file_path)

    with open(file_path, 'r') as stream:
        for _ in range(times):
            data = stream.readline()
            assert yaml.safe_load(data) == {key: msg if not isinstance(msg, tuple) else list(msg)}


def test_write_exception(msg_local_config):
    msg_local_config.files_write = MagicMock()
    msg_local_config.files_write.__contains__.side_effect = Exception()
    
    with pytest.raises(MsgException) as excinfo:
        msg_local_config.write("test", "test", "test")
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR


@pytest.mark.parametrize("topics, datas", [
    (["topic"], [{"key": "value"}]),
    (["topic1"], [{"key": "value"}]),
    (["topic2"], [{"key": "value"}]),
    (["topic", "topic1"], [{"key": "value"}]),
    (["topic", "topic2"], [{"key": "value"}]),
    (["topic1", "topic2"], [{"key": "value"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}]),
    (["topic"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}])])
def test_read(msg_local_with_data, topics, datas):
    def write_to_topic(topics, datas):
        # Allow msglocal to block while waiting
        time.sleep(2)
        for topic in topics:
            for data in datas:
                with open(msg_local_with_data.path + topic, "a+") as fp:
                    yaml.safe_dump(data, fp, default_flow_style=True, width=20000)
                    fp.flush()

    # If file is not opened first, the messages written won't be seen
    for topic in topics:
        if topic not in msg_local_with_data.files_read:
            msg_local_with_data.read(topic, blocks=False)

    t = threading.Thread(target=write_to_topic, args=(topics, datas))
    t.start()

    for topic in topics:
        for data in datas:
            recv_topic, recv_key, recv_msg = msg_local_with_data.read(topic)
            key = list(data.keys())[0]
            val = data[key]
            assert recv_topic == topic
            assert recv_key == key
            assert recv_msg == val
    t.join()


@pytest.mark.parametrize("topics, datas", [
    (["topic"], [{"key": "value"}]),
    (["topic1"], [{"key": "value"}]),
    (["topic2"], [{"key": "value"}]),
    (["topic", "topic1"], [{"key": "value"}]),
    (["topic", "topic2"], [{"key": "value"}]),
    (["topic1", "topic2"], [{"key": "value"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}]),
    (["topic"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}])])
def test_read_non_block(msg_local_with_data, topics, datas):
    def write_to_topic(topics, datas):
        for topic in topics:
            for data in datas:
                with open(msg_local_with_data.path + topic, "a+") as fp:
                    yaml.safe_dump(data, fp, default_flow_style=True, width=20000)
                    fp.flush()

    # If file is not opened first, the messages written won't be seen
    for topic in topics:
        if topic not in msg_local_with_data.files_read:
            msg_local_with_data.read(topic, blocks=False)

    t = threading.Thread(target=write_to_topic, args=(topics, datas))
    t.start()
    t.join()

    for topic in topics:
        for data in datas:
            recv_topic, recv_key, recv_msg = msg_local_with_data.read(topic, blocks=False)
            key = list(data.keys())[0]
            val = data[key]
            assert recv_topic == topic
            assert recv_key == key
            assert recv_msg == val


@pytest.mark.parametrize("topics, datas", [
    (["topic"], [{"key": "value"}]),
    (["topic1"], [{"key": "value"}]),
    (["topic2"], [{"key": "value"}]),
    (["topic", "topic1"], [{"key": "value"}]),
    (["topic", "topic2"], [{"key": "value"}]),
    (["topic1", "topic2"], [{"key": "value"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}]),
    (["topic"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}])])
def test_read_non_block_none(msg_local_with_data, topics, datas):
    def write_to_topic(topics, datas):
        time.sleep(2)
        for topic in topics:
            for data in datas:
                with open(msg_local_with_data.path + topic, "a+") as fp:
                    yaml.safe_dump(data, fp, default_flow_style=True, width=20000)
                    fp.flush()
    # If file is not opened first, the messages written won't be seen
    for topic in topics:
        if topic not in msg_local_with_data.files_read:
            msg_local_with_data.read(topic, blocks=False)
    t = threading.Thread(target=write_to_topic, args=(topics, datas))
    t.start()

    for topic in topics:
        recv_data = msg_local_with_data.read(topic, blocks=False)
        assert recv_data is None
    t.join()


@pytest.mark.parametrize("blocks", [
    (True),
    (False)])
def test_read_exception(msg_local_with_data, blocks):
    msg_local_with_data.files_read = MagicMock()
    msg_local_with_data.files_read.__contains__.side_effect = Exception()

    with pytest.raises(MsgException) as excinfo:
        msg_local_with_data.read("topic1", blocks=blocks)
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR


@pytest.mark.parametrize("topics, datas", [
    (["topic"], [{"key": "value"}]),
    (["topic1"], [{"key": "value"}]),
    (["topic2"], [{"key": "value"}]),
    (["topic", "topic1"], [{"key": "value"}]),
    (["topic", "topic2"], [{"key": "value"}]),
    (["topic1", "topic2"], [{"key": "value"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}]),
    (["topic"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}]),
    (["topic", "topic1", "topic2"], [{"key": "value"}, {"key1": "value1"}])])
def test_aioread(msg_local_with_data, event_loop, topics, datas):
    def write_to_topic(topics, datas):
        time.sleep(2)
        for topic in topics:
            for data in datas:
                with open(msg_local_with_data.path + topic, "a+") as fp:
                    yaml.safe_dump(data, fp, default_flow_style=True, width=20000)
                    fp.flush()
    # If file is not opened first, the messages written won't be seen
    for topic in topics:
        if topic not in msg_local_with_data.files_read:
            msg_local_with_data.read(topic, blocks=False)

    t = threading.Thread(target=write_to_topic, args=(topics, datas))
    t.start()
    for topic in topics:
        for data in datas:
            recv = event_loop.run_until_complete(msg_local_with_data.aioread(topic, event_loop))
            recv_topic, recv_key, recv_msg = recv
            key = list(data.keys())[0]
            val = data[key]
            assert recv_topic == topic
            assert recv_key == key
            assert recv_msg == val
    t.join()


def test_aioread_exception(msg_local_with_data, event_loop):
    msg_local_with_data.files_read = MagicMock()
    msg_local_with_data.files_read.__contains__.side_effect = Exception()

    with pytest.raises(MsgException) as excinfo:
        event_loop.run_until_complete(msg_local_with_data.aioread("topic1", event_loop))
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR


def test_aioread_general_exception(msg_local_with_data, event_loop):
    msg_local_with_data.read = MagicMock()
    msg_local_with_data.read.side_effect = Exception()

    with pytest.raises(MsgException) as excinfo:
        event_loop.run_until_complete(msg_local_with_data.aioread("topic1", event_loop))
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR


@pytest.mark.parametrize("topic, key, msg", [
    ("test_topic", "test_key", "test_msg"),
    ("test", "test_key", "test_msg"),
    ("test_topic", "test", "test_msg"),
    ("test_topic", "test_key", "test"),
    ("test_topic", "test_list", ["a", "b", "c"]),
    ("test_topic", "test_tuple", ("c", "b", "a")),
    ("test_topic", "test_dict", {"a": 1, "b": 2, "c": 3}),
    ("test_topic", "test_number", 123),
    ("test_topic", "test_float", 1.23),
    ("test_topic", "test_boolean", True),
    ("test_topic", "test_none", None)])
def test_aiowrite(msg_local_config, event_loop, topic, key, msg):
    file_path = msg_local_config.path + topic
    event_loop.run_until_complete(msg_local_config.aiowrite(topic, key, msg))
    assert os.path.exists(file_path)

    with open(file_path, 'r') as stream:
        assert yaml.safe_load(stream) == {key: msg if not isinstance(msg, tuple) else list(msg)}


@pytest.mark.parametrize("topic, key, msg, times", [
    ("test_topic", "test_key", "test_msg", 2),
    ("test", "test_key", "test_msg", 3),
    ("test_topic", "test", "test_msg", 4),
    ("test_topic", "test_key", "test", 2),
    ("test_topic", "test_list", ["a", "b", "c"], 3),
    ("test_topic", "test_tuple", ("c", "b", "a"), 4),
    ("test_topic", "test_dict", {"a": 1, "b": 2, "c": 3}, 2),
    ("test_topic", "test_number", 123, 3),
    ("test_topic", "test_float", 1.23, 4),
    ("test_topic", "test_boolean", True, 2),
    ("test_topic", "test_none", None, 3)])
def test_aiowrite_with_multiple_calls(msg_local_config, event_loop, topic, key, msg, times):
    file_path = msg_local_config.path + topic
    for _ in range(times):
        event_loop.run_until_complete(msg_local_config.aiowrite(topic, key, msg))
    assert os.path.exists(file_path)

    with open(file_path, 'r') as stream:
        for _ in range(times):
            data = stream.readline()
            assert yaml.safe_load(data) == {key: msg if not isinstance(msg, tuple) else list(msg)}


def test_aiowrite_exception(msg_local_config, event_loop):
    msg_local_config.files_write = MagicMock()
    msg_local_config.files_write.__contains__.side_effect = Exception()
    
    with pytest.raises(MsgException) as excinfo:
        event_loop.run_until_complete(msg_local_config.aiowrite("test", "test", "test"))
    assert str(excinfo.value).startswith(empty_exception_message())
    assert excinfo.value.http_code == http.HTTPStatus.INTERNAL_SERVER_ERROR
