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

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"

"""
This emulated kafka bus by just using a shared file system. Useful for testing or devops.
One file is used per topic. Only one producer and one consumer is allowed per topic. Both consumer and producer
access to the same file. e.g. same volume if running with docker.
One text line per message is used in yaml format.
"""


class MsgLocal(MsgBase):

    def __init__(self, logger_name='msg'):
        self.logger = logging.getLogger(logger_name)
        self.path = None
        # create a different file for each topic
        self.files_read = {}
        self.files_write = {}
        self.buffer = {}

    def connect(self, config):
        try:
            if "logger_name" in config:
                self.logger = logging.getLogger(config["logger_name"])
            self.path = config["path"]
            if not self.path.endswith("/"):
                self.path += "/"
            if not os.path.exists(self.path):
                os.mkdir(self.path)
        except MsgException:
            raise
        except Exception as e:  # TODO refine
            raise MsgException(str(e))

    def disconnect(self):
        for f in self.files_read.values():
            try:
                f.close()
            except Exception:  # TODO refine
                pass
        for f in self.files_write.values():
            try:
                f.close()
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
            if topic not in self.files_write:
                self.files_write[topic] = open(self.path + topic, "a+")
            yaml.safe_dump({key: msg}, self.files_write[topic], default_flow_style=True, width=20000)
            self.files_write[topic].flush()
        except Exception as e:  # TODO refine
            raise MsgException(str(e))

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
            raise MsgException(str(e))

    async def aioread(self, topic, loop):
        """
        Asyncio read from one or several topics. It blocks
        :param topic: can be str: single topic; or str list: several topics
        :param loop: asyncio loop
        :return: topic, key, message
        """
        try:
            while True:
                msg = self.read(topic, blocks=False)
                if msg:
                    return msg
                await asyncio.sleep(2, loop=loop)
        except MsgException:
            raise
        except Exception as e:  # TODO refine
            raise MsgException(str(e))

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
