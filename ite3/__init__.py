
from .pager import Pager


class SQLite3IO:
    def open(self, path, mode):
        self.pager = Pager()
        self.pager.open(path, mode)
        return self

    def close(self):
        self.pager.close()


def open(path, mode):
    return SQLite3IO().open(path, mode)
