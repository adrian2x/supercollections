import os
from contextlib import suppress
from collections.abc import MutableMapping

from filelock import FileLock


class FileDict(MutableMapping):
    """ File based dictionary

    A dictionary-like object based on the file system rather than in-memory hash tables. It is
    persistent and shareable between processes.
    """

    def __init__(self, dirname, **kwargs):
        self.dirname = dirname
        with suppress(FileExistsError):
            os.makedirs(dirname)
        self.update(**kwargs)

    def __getitem__(self, key):
        fullname = os.path.join(self.dirname, key)
        try:
            with open(fullname) as file:
                return file.read()
        except IOError:
            raise KeyError(key) from None

    def __setitem__(self, key, value):
        fullname = os.path.join(self.dirname, key)
        # Create a lock file to prevent corruption
        lock = FileLock(fullname + ".lock")
        with lock, open(fullname, 'w+') as file:
            file.write(value)

    def __delitem__(self, key):
        fullname = os.path.join(self.dirname, key)
        try:
            os.remove(fullname)
        except IOError:
            raise KeyError(key) from None

    def __len__(self):
        return len(os.listdir(self.dirname))

    def __iter__(self):
        return iter(os.listdir(self.dirname))

    def __repr__(self):
        return f'FileDict{tuple(self.items())}'
