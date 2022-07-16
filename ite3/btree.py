import binascii
from .bits import read_varint
from .payload import CellPayload

# https://www.sqlite.org/fileformat.html
# https://sqlite.org/src/file?name=src/btreeInt.h&ci=trunk

BTREE_PAGE_TYPE_INTERIOR_INDEX = 2
BTREE_PAGE_TYPE_INTERIOR_TABLE = 5
BTREE_PAGE_TYPE_LEAF_INDEX = 10
BTREE_PAGE_TYPE_LEAF_TABLE = 13



def calculate_cell_in_page_bytes(ln, pagesize, max_in_page_payload):
    u = page_size
    p = ln
    x = max_in_page_payload
    m = ((u - 12) * 32 // 255) - 23
    k = m + ((p - m) % (u - 4))

    if p <= x:
        return ln
    elif k <= x:
        return k
    else:
        return m


def parse_payload(payload_len, cell_content, page_size, max_in_page_payload):
    overflow = 0
    in_page_bytes = calculate_cell_in_page_bytes(payload_len, page_size, max_in_page_payload)
    if payload_len < 0:
        raise ValueError("parse_payload()")

    if in_page_bytes == payload_len:
        return CellPayload(payload_len, cell_content, 0)

    if len(cell_content) < in_page_bytes+4:
        raise ValueError("parse_payload()")

    first_content = cell_content[:in_page_bytes]
    overflow = int.from_bytes(cell_content[in_page_bytes:in_page_bytes+4], 'big')
    if overflow == 0:
        raise ValueError("parse_payload()")

    return CellPayload(payload_len, first_content, overflow)


class TableLeafCell:
    def __init__(self, btree, cell_pointer):
        page_size = len(btree.data)
        c = btree.data[cell_pointer:]
        payload_len, ln = read_varint(c)
        c = c[ln:]
        rowid, ln = read_varint(c)
        c = c[ln:]

        self.left = rowid
        self.payload = parse_payload(payload_len, c, page_size, page_size-35)


class TableInteriorCell:
    def __init__(self, btree, cell_pointer):
        c = btree.data[cell_pointer:]
        left = int.from_bytes(c[:4], 'big')
        key, _ = read_varint(c[4:])

        self.left = left
        self.key = key


class IndexLeafCell:
    def __init__(self, btree, cell_pointer):
        page_size = len(btree.data)
        c = btree.data[cell_pointer:]
        payload_len, ln = read_varint(c)
        c = c[ln:]

        self.payload = parse_payload(payload_len, c, page_size, ((page_size-12)*64/255)-23)


class IndexInteriorCell:
    def __init__(self, btree, cell_pointer):
        page_size = len(btree.data)
        c = btree.data[cell_pointer:]
        left = int.from_bytes(c[:4], 'big')
        payload_len, ln = read_varint(c)
        c = c[ln:]

        self.left = left
        self.payload = parse_payload(payload_len, c, page_size, ((page_size-12)*64/255)-23)


class Btree:
    def __init__(self, page):
        self.page = page
        offset = 0
        if page.pgno == 0:
            offset = 100
        self.btree_page_type = page.data[offset]
        self.free_block_offset = int.from_bytes(page.data[offset+1:offset+3], 'big')
        self.number_of_cells = int.from_bytes(page.data[offset+3:offset+5], 'big')
        self.first_byte_of_cell_content = int.from_bytes(page.data[offset+5:offset+7], 'big')
        self.number_of_fragmented_free_bytes = page.data[offset+7]
        if self.btree_page_type in (BTREE_PAGE_TYPE_LEAF_INDEX, BTREE_PAGE_TYPE_LEAF_TABLE):
            self.right_child = None
            offset += 8
        else:
            self.right_child = int.from_bytes(page.data[offset+8:offset+12], 'big')
            offset += 12
        self.cell_offset = offset

    def get_cell_pointers(self):
        cell_pointers = []
        for i in range(self.cell_offset, self.cell_offset+self.number_of_cells*2, 2):
            cell_pointers.append(int.from_bytes(self.page.data[i:i+2], 'big'))
        return cell_pointers

    def get_cell_content(self, i):
        cells = self.get_cell_pointers()
        cell_pointer = cells[i]
        try:
            next_pointer = min(list(filter(lambda v: v > cell_pointer, cells)))
        except ValueError:
            # last content
            next_pointer = len(self.page.data)
        return self.page.data[cell_pointer:next_pointer]

    def _dump(self):
        print("  Btree:")
        print("    pgno=", self.page.pgno)
        print("    is_dirty=", self.page.is_dirty)
        print("    btree_page_type=", self.btree_page_type)
        print("    free_block_offset=", self.free_block_offset)
        print("    number_of_cells=", self.number_of_cells)
        print("    first_byte_of_cell_content=", self.first_byte_of_cell_content)
        print("    number_of_fragmented_free_bytes", self.number_of_fragmented_free_bytes)
        print("    cell_pointers=", self.get_cell_pointers())

    def _dump_cell_content(self, cell):
        print("   ", binascii.hexlify(cell).decode('utf-8'))
        s = ''
        for c in cell:
            s += chr(c) if (c >= 32 and c < 128) else '.'
        print("    [{}]".format(s))

    def _dump_cell_contents(self):
        for i in range(self.number_of_cells):
            self._dump_cell_content(self.get_cell_content(i))
