import collections
from .btree import Btree

# https://www.sqlite.org/fileformat.html



class PageIter:
    def __init__(self, pager):
        self.pager = pager
        self.pgno = 0

    def __next__(self):
        page = self.pager.get_page(self.pgno)
        if not page:
            raise StopIteration()
        self.pgno += 1
        return page


class Page:
    def __init__(self, pgno, data, is_dirty):
        self.pgno = pgno
        self.data = data
        self.is_dirty = is_dirty



class Pager:
    def __init__(self, fileobj=None):
        self.path = None
        self.page_size = 0
        self.page = collections.OrderedDict()
        self.next_pgno = 0
        self.fileobj = fileobj
        if self.fileobj:
            self._read_header()

    def __iter__(self):
        return PageIter(self)

    def _dump(self):
        print("Pager:")
        print("  path=", self.path)
        print("  page_size=", self.page_size)

        print("  file_change_counter=", self.file_change_counter)
        print("  header_btree_count=", self.header_btree_count)
        print("  pgno_first_freelist=", self.pgno_first_freelist)
        print("  num_freelist_pages=", self.num_freelist_pages)
        print("  schema_cookie=", self.schema_cookie)
        print("  schema_format_no=", self.schema_format_no)
        print("  page_cache_size=", self.page_cache_size)
        print("  text_encoding=", self.text_encoding)
        print("  next_pgno=", self.next_pgno)

    def _read_header(self):
        self.page.clear()

        magic = self.fileobj.read(16)
        if magic != b"SQLite format 3\x00":
            self.fileobj.close()
            raise ValueError("Invalid Magic header")
        self.page_size = int.from_bytes(self.fileobj.read(2), 'big')
        if self.page_size == 1:
            self.page_size = 65536

        self.fileobj.seek(24, 0)
        self.file_change_counter = int.from_bytes(self.fileobj.read(4), 'big')
        self.header_btree_count = int.from_bytes(self.fileobj.read(4), 'big')
        self.pgno_first_freelist = int.from_bytes(self.fileobj.read(4), 'big')
        self.num_freelist_pages = int.from_bytes(self.fileobj.read(4), 'big')
        self.schema_cookie = int.from_bytes(self.fileobj.read(4), 'big')
        self.schema_format_no = int.from_bytes(self.fileobj.read(4), 'big')
        self.page_cache_size = int.from_bytes(self.fileobj.read(4), 'big')
        self.fileobj.read(4)   # The page number of the largest root b-tree page
        self.text_encoding = int.from_bytes(self.fileobj.read(4), 'big')
        self.next_pgno = self.header_btree_count

    def open(self, path, mode='r'):
        if mode == 'r':
            mode = 'rb'
        elif mode == 'w':
            mode = 'r+b'
        else:
            raise ValueError("Invalid Mode: {}".format(mode))
        if self.fileobj:
            self.fileobj.close()
        self.path = path
        self.fileobj = open(path, mode)
        self._read_header()

    def close(self, ):
        self.fileobj.close()
        self.file_size = 0
        self.page_size = 0
        self.page.clear()
        self.fileobj = None

    def _read_block(self, pgno):
        self.fileobj.seek(pgno * self.page_size, 0)
        data = self.fileobj.read(self.page_size)
        return Page(pgno, data, is_dirty=False)

    def get_page(self, pgno=-1):
        if pgno == -1:
            # New Btree
            data = b'\x00' * self.page_size
            self.page[self.next_pgno] = data
            self.next_pgno += 1
        elif pgno < self.next_pgno:
            btree = self.page.get(pgno)
            if not btree:
                page = self._read_block(pgno)
                self.page[pgno] = btree
        else:
            page = None
        return page

    def _flush_page(self, pgno, page):
        if btree.is_dirty:
            self.fileobj.seek(pgno * self.page_size, 0)
            self.fileobj.write(page.data)
            btree.is_dirty = False

    def flush_btrees(self):
        for pgno, page in self.pagres.items():
            self._flush_page(pgno, btree)

    def __exit__(self, exc, value, traceback):
        self.close()
