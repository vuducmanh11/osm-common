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

from io import BytesIO, StringIO
from pymongo import MongoClient
from gridfs import GridFSBucket, errors
import logging
from http import HTTPStatus
import os
from osm_common.fsbase import FsBase, FsException

__author__ = "Eduardo Sousa <eduardo.sousa@canonical.com>"


class GridByteStream(BytesIO):
    def __init__(self, filename, fs, mode):
        BytesIO.__init__(self)
        self._id = None
        self.filename = filename
        self.fs = fs
        self.mode = mode

        self.__initialize__()

    def __initialize__(self):
        grid_file = None

        cursor = self.fs.find({"filename": self.filename})

        for requested_file in cursor:
            exception_file = next(cursor, None)

            if exception_file:
                raise FsException("Multiple files found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

            if requested_file.metadata["type"] == "file":
                grid_file = requested_file
            else:
                raise FsException("Type isn't file", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

        if grid_file:
            self._id = grid_file._id
            self.fs.download_to_stream(self._id, self)

            if "r" in self.mode:
                self.seek(0, 0)

    def close(self):
        if "r" in self.mode:
            super(GridByteStream, self).close()
            return

        if self._id:
            self.fs.delete(self._id)

        cursor = self.fs.find({
            "filename": self.filename.split("/")[0],
            "metadata": {"type": "dir"}})

        parent_dir = next(cursor, None)

        if not parent_dir:
            parent_dir_name = self.filename.split("/")[0]
            self.filename = self.filename.replace(parent_dir_name, parent_dir_name[:-1], 1)

        self.seek(0, 0)
        if self._id:
            self.fs.upload_from_stream_with_id(
                self._id,
                self.filename,
                self,
                metadata={"type": "file"}
            )
        else:
            self.fs.upload_from_stream(
                self.filename,
                self,
                metadata={"type": "file"}
            )
        super(GridByteStream, self).close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class GridStringStream(StringIO):
    def __init__(self, filename, fs, mode):
        StringIO.__init__(self)
        self._id = None
        self.filename = filename
        self.fs = fs
        self.mode = mode

        self.__initialize__()

    def __initialize__(self):
        grid_file = None

        cursor = self.fs.find({"filename": self.filename})

        for requested_file in cursor:
            exception_file = next(cursor, None)

            if exception_file:
                raise FsException("Multiple files found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

            if requested_file.metadata["type"] == "file":
                grid_file = requested_file
            else:
                raise FsException("File type isn't file", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

        if grid_file:
            stream = BytesIO()
            self._id = grid_file._id
            self.fs.download_to_stream(self._id, stream)
            stream.seek(0)
            self.write(stream.read().decode("utf-8"))
            stream.close()

            if "r" in self.mode:
                self.seek(0, 0)

    def close(self):
        if "r" in self.mode:
            super(GridStringStream, self).close()
            return

        if self._id:
            self.fs.delete(self._id)

        cursor = self.fs.find({
            "filename": self.filename.split("/")[0],
            "metadata": {"type": "dir"}})

        parent_dir = next(cursor, None)

        if not parent_dir:
            parent_dir_name = self.filename.split("/")[0]
            self.filename = self.filename.replace(parent_dir_name, parent_dir_name[:-1], 1)

        self.seek(0, 0)
        stream = BytesIO()
        stream.write(self.read().encode("utf-8"))
        stream.seek(0, 0)
        if self._id:
            self.fs.upload_from_stream_with_id(
                self._id,
                self.filename,
                stream,
                metadata={"type": "file"}
            )
        else:
            self.fs.upload_from_stream(
                self.filename,
                stream,
                metadata={"type": "file"}
            )
        stream.close()
        super(GridStringStream, self).close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FsMongo(FsBase):

    def __init__(self, logger_name='fs', lock=False):
        super().__init__(logger_name, lock)
        self.path = None
        self.client = None
        self.fs = None

    def __update_local_fs(self):
        dir_cursor = self.fs.find({"metadata.type": "dir"}, no_cursor_timeout=True)

        for directory in dir_cursor:
            os.makedirs(self.path + directory.filename, exist_ok=True)

        file_cursor = self.fs.find({"metadata.type": "file"}, no_cursor_timeout=True)

        for writing_file in file_cursor:
            file_path = self.path + writing_file.filename
            file_stream = open(file_path, 'wb+')
            self.fs.download_to_stream(writing_file._id, file_stream)
            file_stream.close()
            if "permissions" in writing_file.metadata:
                os.chmod(file_path, writing_file.metadata["permissions"])

    def get_params(self):
        self.__update_local_fs()

        return {"fs": "mongo", "path": self.path}

    def fs_connect(self, config):
        try:
            if "logger_name" in config:
                self.logger = logging.getLogger(config["logger_name"])
            if "path" in config:
                self.path = config["path"]
            else:
                raise FsException("Missing parameter \"path\"")
            if not self.path.endswith("/"):
                self.path += "/"
            if not os.path.exists(self.path):
                raise FsException("Invalid configuration param at '[storage]': path '{}' does not exist".format(
                    config["path"]))
            elif not os.access(self.path, os.W_OK):
                raise FsException("Invalid configuration param at '[storage]': path '{}' is not writable".format(
                    config["path"]))
            if all(key in config.keys() for key in ["uri", "collection"]):
                self.client = MongoClient(config["uri"])
                self.fs = GridFSBucket(self.client[config["collection"]])
            elif all(key in config.keys() for key in ["host", "port", "collection"]):
                self.client = MongoClient(config["host"], config["port"])
                self.fs = GridFSBucket(self.client[config["collection"]])
            else:
                if "collection" not in config.keys():
                    raise FsException("Missing parameter \"collection\"")
                else:
                    raise FsException("Missing parameters: \"uri\" or \"host\" + \"port\"")
        except FsException:
            raise
        except Exception as e:  # TODO refine
            raise FsException(str(e))

    def fs_disconnect(self):
        pass  # TODO

    def mkdir(self, folder):
        """
        Creates a folder or parent object location
        :param folder:
        :return: None or raises an exception
        """
        try:
            self.fs.upload_from_stream(
                folder, BytesIO(), metadata={"type": "dir"})
        except errors.FileExists:  # make it idempotent
            pass
        except Exception as e:
            raise FsException(str(e), http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

    def dir_rename(self, src, dst):
        """
        Rename one directory name. If dst exist, it replaces (deletes) existing directory
        :param src: source directory
        :param dst: destination directory
        :return: None or raises and exception
        """
        try:
            dst_cursor = self.fs.find(
                {"filename": {"$regex": "^{}(/|$)".format(dst)}},
                no_cursor_timeout=True)

            for dst_file in dst_cursor:
                self.fs.delete(dst_file._id)

            src_cursor = self.fs.find(
                {"filename": {"$regex": "^{}(/|$)".format(src)}},
                no_cursor_timeout=True)

            for src_file in src_cursor:
                self.fs.rename(src_file._id, src_file.filename.replace(src, dst, 1))
        except Exception as e:
            raise FsException(str(e), http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

    def file_exists(self, storage, mode=None):
        """
        Indicates if "storage" file exist
        :param storage: can be a str or a str list
        :param mode: can be 'file' exist as a regular file; 'dir' exists as a directory or; 'None' just exists
        :return: True, False
        """
        f = storage if isinstance(storage, str) else "/".join(storage)

        cursor = self.fs.find({"filename": f})

        for requested_file in cursor:
            exception_file = next(cursor, None)

            if exception_file:
                raise FsException("Multiple files found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

            if requested_file.metadata["type"] == mode:
                return True

        return False

    def file_size(self, storage):
        """
        return file size
        :param storage: can be a str or a str list
        :return: file size
        """
        f = storage if isinstance(storage, str) else "/".join(storage)

        cursor = self.fs.find({"filename": f})

        for requested_file in cursor:
            exception_file = next(cursor, None)

            if exception_file:
                raise FsException("Multiple files found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

            return requested_file.length

    def file_extract(self, tar_object, path):
        """
        extract a tar file
        :param tar_object: object of type tar
        :param path: can be a str or a str list, or a tar object where to extract the tar_object
        :return: None
        """
        f = path if isinstance(path, str) else "/".join(path)

        for member in tar_object.getmembers():
            if member.isfile():
                stream = tar_object.extractfile(member)
            else:
                stream = BytesIO()

            metadata = {
                "type": "file" if member.isfile() else "dir",
                "permissions": member.mode
            }

            self.fs.upload_from_stream(
                f + "/" + member.name,
                stream,
                metadata=metadata
            )

            stream.close()

    def file_open(self, storage, mode):
        """
        Open a file
        :param storage: can be a str or list of str
        :param mode: file mode
        :return: file object
        """
        try:
            f = storage if isinstance(storage, str) else "/".join(storage)

            if "b" in mode:
                return GridByteStream(f, self.fs, mode)
            else:
                return GridStringStream(f, self.fs, mode)
        except errors.NoFile:
            raise FsException("File {} does not exist".format(f), http_code=HTTPStatus.NOT_FOUND)
        except IOError:
            raise FsException("File {} cannot be opened".format(f), http_code=HTTPStatus.BAD_REQUEST)

    def dir_ls(self, storage):
        """
        return folder content
        :param storage: can be a str or list of str
        :return: folder content
        """
        try:
            f = storage if isinstance(storage, str) else "/".join(storage)

            files = []
            dir_cursor = self.fs.find({"filename": f})
            for requested_dir in dir_cursor:
                exception_dir = next(dir_cursor, None)

                if exception_dir:
                    raise FsException("Multiple directories found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

                if requested_dir.metadata["type"] != "dir":
                    raise FsException("File {} does not exist".format(f), http_code=HTTPStatus.NOT_FOUND)

                files_cursor = self.fs.find({"filename": {"$regex": "^{}/([^/])*".format(f)}})
                for children_file in files_cursor:
                    files += [children_file.filename.replace(f + '/', '', 1)]

            return files
        except IOError:
            raise FsException("File {} cannot be opened".format(f), http_code=HTTPStatus.BAD_REQUEST)

    def file_delete(self, storage, ignore_non_exist=False):
        """
        Delete storage content recursively
        :param storage: can be a str or list of str
        :param ignore_non_exist: not raise exception if storage does not exist
        :return: None
        """
        try:
            f = storage if isinstance(storage, str) else "/".join(storage)

            file_cursor = self.fs.find({"filename": f})
            found = False
            for requested_file in file_cursor:
                found = True
                exception_file = next(file_cursor, None)

                if exception_file:
                    raise FsException("Multiple files found", http_code=HTTPStatus.INTERNAL_SERVER_ERROR)

                if requested_file.metadata["type"] == "dir":
                    dir_cursor = self.fs.find({"filename": {"$regex": "^{}".format(f)}})

                    for tmp in dir_cursor:
                        self.fs.delete(tmp._id)
                else:
                    self.fs.delete(requested_file._id)
            if not found and not ignore_non_exist:
                raise FsException("File {} does not exist".format(storage), http_code=HTTPStatus.NOT_FOUND)    
        except IOError as e:
            raise FsException("File {} cannot be deleted: {}".format(f, e), http_code=HTTPStatus.INTERNAL_SERVER_ERROR)
