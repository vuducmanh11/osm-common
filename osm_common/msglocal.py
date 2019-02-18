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
import os
import yaml
import asyncio
from osm_common.msgbase import MsgBase, MsgException
from time import sleep
from http import HTTPStatus

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"

"""
This emulated kafka bus by just using a shared file system. Useful for testing or devops.
One file is used per topic. Only one producer and one consumer is allowed per topic. Both consumer and producer
access to the same file. e.g. same volume if running with docker.
One text line per message is used in yaml format.
"""


class MsgLocal(MsgBase):

    def __init__(self, logger_name='msg', lock=False):
        super().__init__(logger_name, lock)
        self.path = None
        # create a different file for each topic
        self.files_read = {}
        self.files_write = {}
        self.buffer = {}
        self.loop = None

    def connect(self, config):
        try:
            if "logger_name" in config:
                self.logger = logging.getLogger(config["logger_name"])
            self.path = config["path"]
            if not self.path.endswith("/"):
                self.path += "/"
            if not os.path.exists(self.path):
                os.mkdir(self.path)
            self.loop = config.get("loop")

        except MsgException:
            raise
        except Exception as e:  # TODO refine
            raise MsgException(str(e), http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

    def disconnect(self):
        for topic, f in self.files_read.items():
            try:
                f.close()
                self.files_read[topic] = None
            except Exception:  # TODO refine
                pass
        for topic, f in self.files_write.items():
            try:
                f.close()
                self.files_write[topic] = None
            except Exception:  # TODO refine
                pass

    def write(self, topic, key, msg):
        """
        Insert a message into topic
        :param topic: topic
        :param key: key text to be inserted
        :param msg: value object to be inserted, can be str, object ...
        :return: None or raises and exception
        """
        try:
            with self.lock:
                if topic not in self.files_write:
                    self.files_write[topic] = open(self.path + topic, "a+")
                yaml.safe_dump({key: msg}, self.files_write[topic], default_flow_style=True, width=20000)
                self.files_write[topic].flush()
        except Exception as e:  # TODO refine
            raise MsgException(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

    def read(self, topic, blocks=True):
        """
        Read from one or several topics. it is non blocking returning None if nothing is available
        :param topic: can be str: single topic; or str list: several topics
        :param blocks: indicates if it should wait and block until a message is present or returns None
        :return: topic, key, message; or None if blocks==True
        """
        try:
            if isinstance(topic, (list, tuple)):
                topic_list = topic
            else:
                topic_list = (topic, )
            while True:
                for single_topic in topic_list:
                    with self.lock:
                        if single_topic not in self.files_read:
                            self.files_read[single_topic] = open(self.path + single_topic, "a+")
                            self.buffer[single_topic] = ""
                        self.buffer[single_topic] += self.files_read[single_topic].readline()
                        if not self.buffer[single_topic].endswith("\n"):
                            continue
                        msg_dict = yaml.load(self.buffer[single_topic])
                        self.buffer[single_topic] = ""
                        assert len(msg_dict) == 1
                        for k, v in msg_dict.items():
                            return single_topic, k, v
                if not blocks:
                    return None
                sleep(2)
        except Exception as e:  # TODO refine
            raise MsgException(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

    async def aioread(self, topic, loop=None, callback=None, aiocallback=None, group_id=None, **kwargs):
        """
        Asyncio read from one or several topics. It blocks
        :param topic: can be str: single topic; or str list: several topics
        :param loop: asyncio loop. To be DEPRECATED! in near future!!!  loop must be provided inside config at connect
        :param callback: synchronous callback function that will handle the message
        :param aiocallback: async callback function that will handle the message
        :param group_id: group_id to use for load balancing. Can be False (set group_id to None), None (use general
                         group_id provided at connect inside config), or a group_id string
        :param kwargs: optional keyword arguments for callback function
        :return: If no callback defined, it returns (topic, key, message)
        """
        _loop = loop or self.loop
        try:
            while True:
                msg = self.read(topic, blocks=False)
                if msg:
                    if callback:
                        callback(*msg, **kwargs)
                    elif aiocallback:
                        await aiocallback(*msg, **kwargs)
                    else:
                        return msg
                await asyncio.sleep(2, loop=_loop)
        except MsgException:
            raise
        except Exception as e:  # TODO refine
            raise MsgException(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

    async def aiowrite(self, topic, key, msg, loop=None):
        """
        Asyncio write. It blocks
        :param topic: str
        :param key: str
        :param msg: message, can be str or yaml
        :param loop: asyncio loop
        :return: nothing if ok or raises an exception
        """
        return self.write(topic, key, msg)
