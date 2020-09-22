#!/usr/bin/env python
# -*- coding: utf-8 -*-
# tarball.py
# Copyright (C) 2020 KunoiSayami
#
# This module is part of Indexable-tarball and is released under
# the AGPL v3 License: https://www.gnu.org/licenses/agpl-3.0.txt
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
import asyncio
import hashlib
import json
import logging
import os
import struct
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import aiofiles

CHUNK_SIZE = 16 * 1024


class IndexableTarballException(Exception):
    pass


class VersionException(IndexableTarballException):
    pass


class FileExist(IndexableTarballException):
    pass


class FileAlreadyClosed(IndexableTarballException):
    pass


class FileNotFound(IndexableTarballException):
    pass


class ChecksumMismatch(IndexableTarballException):
    pass


class ModeMismatch(IndexableTarballException):
    pass


class FileNameDuplicate(IndexableTarballException):
    pass


@dataclass
class FileProperties:
    checksum: str
    offset: int
    length: int
    relative_path: str

    @classmethod
    def create(cls, obj: Tuple[str, int, int, str]) -> 'FileProperties':
        return FileProperties(*obj)

    def dump(self) -> Tuple[str, int, int, str]:
        return self.checksum, self.offset, self.length, self.relative_path


class IndexableTarball:
    VERSION: int = 1

    def __init__(self, file_name: str, is_write: bool = False, *, file_obj: aiofiles.threadpool.AsyncBufferedIOBase):
        self.logger: logging.Logger = logging.getLogger('IndexableTarball')
        self.logger.setLevel(logging.DEBUG)
        self.file_name: str = file_name
        self.is_write: bool = is_write
        self._file_struct: Dict[str, FileProperties] = {}
        self._file_obj: aiofiles.threadpool.AsyncBufferedIOBase = file_obj
        self._lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    async def create(cls, file_name: str) -> 'IndexableTarball':
        self = cls(file_name, True, file_obj=await aiofiles.open(file_name, 'wb'))
        await self._pre_write()
        return self

    @classmethod
    async def load(cls, file_name: str) -> 'IndexableTarball':
        self = cls(file_name, False, file_obj=await aiofiles.open(file_name, 'rb'))
        await self._load_file_struct()
        return self

    async def _pre_write(self) -> None:
        await self._file_obj.write(struct.pack('<HQ22p', self.VERSION, 0, b''))
        self.logger.debug('File header written! %d', await self._file_obj.tell())

    async def _load_file_struct(self) -> None:
        await self._file_obj.seek(0)
        obj = await self._file_obj.read(struct.calcsize('<HQ22p'))
        _ver, struct_offset, _reserved = struct.unpack('<HQ22p', obj)
        if _ver != self.VERSION:
            raise VersionException(f'Except version: {self.VERSION} but {_ver} found')
        await self._file_obj.seek(struct_offset)
        self._file_struct = {key: FileProperties.create(value) for key, value in
                             json.loads((await self._file_obj.read()).decode()).items()}

    async def write(self, input_file_name: str, relative_path: str = '') -> None:
        h = hashlib.sha256()
        if input_file_name in self._file_struct:
            self.logger.critical('Find duplicate name')
            return
        async with self._lock:
            current_pos = await self._file_obj.tell()
            async with aiofiles.open(input_file_name, 'rb') as fin:
                while chunk := await fin.read(CHUNK_SIZE):
                    h.update(chunk)
                    await self._file_obj.write(chunk)
            for file_name, file_properties in self._file_struct.items():
                if file_properties.checksum == h.hexdigest():
                    await self._file_obj.seek(current_pos)
                    await self._file_obj.truncate()
                    raise FileExistsError(f'File {input_file_name} exists: {file_name}: {file_properties.dump()}')
            self._file_struct.update({input_file_name: FileProperties(
                h.hexdigest(), current_pos, await self._file_obj.tell() - current_pos, relative_path)})
        self.logger.debug('Written %s, (%s)', input_file_name, self._file_struct.get(input_file_name))

    async def read(self, index_file_name: str, ignore_relative_path: bool = False,
                   override_relative_path: Optional[str] = None) -> int:
        _file: Optional[FileProperties] = None
        for file_name, file_properties in self._file_struct.items():
            if file_name == index_file_name:
                _file = file_properties
                break
        if _file is None:
            raise FileNotFoundError()
        h = hashlib.sha256()

        if override_relative_path:
            _file.relative_path = override_relative_path
        output_path = index_file_name if ignore_relative_path else os.path.join(_file.relative_path, index_file_name)

        async with self._lock:
            await self._file_obj.seek(_file.offset)
            async with aiofiles.open(output_path, 'wb') as fout:
                for _ in range(_file.length//CHUNK_SIZE):
                    chunk = await self._file_obj.read(CHUNK_SIZE)
                    h.update(chunk)
                    await fout.write(chunk)
                if last_size := _file.length % CHUNK_SIZE:
                    chunk = await self._file_obj.read(last_size)
                    h.update(chunk)
                    await fout.write(chunk)
            if h.hexdigest() != _file.checksum:
                raise ChecksumMismatch()

        return _file.length

    async def _write_file_struct(self) -> None:
        async with self._lock:
            assert await self._check_offset()
            current_offset = await self._file_obj.tell()
            await self._file_obj.write(json.dumps({k: v.dump() for k, v in self._file_struct.items()}).encode())
            await self._file_obj.seek(struct.calcsize('H'))
            await self._file_obj.write(struct.pack('Q', current_offset))
            self.logger.debug('Offset written %d (%d)', current_offset, await self._file_obj.tell())
            await self._file_obj.seek(current_offset)

    async def _check_offset(self) -> bool:
        _n = -0x7ffffff
        for _key, file_properties in self._file_struct.items():
            _n = max(file_properties.offset + file_properties.length, _n)
        return _n == await self._file_obj.tell()

    async def close(self) -> None:
        if self._file_obj is not None:
            if self.is_write:
                await self._write_file_struct()
            await self._file_obj.close()
            self._file_obj = None
        else:
            raise FileAlreadyClosed()

    def check_checksum(self, files: Dict[str, str]):
        for k, v in files.items():
            self.logger.debug('Checking %s(%s), %s', k, v, self._file_struct.get(k).checksum == v)


def generate_str(size: int = 10) -> str:
    return ''.join(chr(ord('a') + random.randint(0, 23)) for _ in range(size))


async def test_file(large: bool = False) -> None:
    p: Dict[str, str] = {}
    for _ in range(20 if large else 100):
        h = hashlib.sha256()
        file_name = f'tmp{generate_str()}'
        if large:
            size = random.randint(1024, 4096) * 1024 * 8
        else:
            size = random.randint(1024, 4096)
        async with aiofiles.open(file_name, 'wb') as fout:
            obj = b''.join(os.urandom(size) for _ in range(random.randint(3, 6)))
            await fout.write(obj)
            h.update(obj)
            del obj
        p.update({file_name: h.hexdigest()})
    await process_all_file_in_dir(p)
    await extract_files(p)


async def process_all_file_in_dir(files: Dict[str, str]) -> None:
    foo = await IndexableTarball.create('foo')
    for filename, _ in files.items():
        await foo.write(filename)
    foo.check_checksum(files)
    await foo.close()


async def extract_files(files: Dict[str, str]) -> None:
    foo = await IndexableTarball.load('foo')
    with tempfile.TemporaryDirectory(prefix='extract', dir='.') as tmpd:
        for filename, checksum in files.items():
            await foo.read(filename, override_relative_path=tmpd)
            assert checksum == await get_checksum(os.path.join(tmpd, filename))
            print(filename, checksum)
    await foo.close()


async def get_checksum(file_path: str) -> str:
    h = hashlib.sha256()
    async with aiofiles.open(file_path, 'rb') as fin:
        while chunk := await fin.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


async def test():
    with tempfile.TemporaryDirectory(prefix='tmp', dir='.') as tmpd:
        os.chdir(tmpd)
        await test_file(True)
        os.chdir('..')

if __name__ == '__main__':
    import coloredlogs
    import tempfile
    import random
    coloredlogs.install(logging.DEBUG,
                        fmt='%(asctime)s,%(msecs)03d - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s')
    asyncio.get_event_loop().run_until_complete(test())
