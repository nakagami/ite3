

class CellPayload:
    def __init__(self, ln, payload, overflow):
        self.ln = ln                    # length
        self.first_payload = payload    # first payload bytes
        self.overflow_pgno = overflow   # overflow pgno

    def get_payload(self, pager):
        "get payload bytes with overflow"
        buf = self.first_payload[:]
        overflow = self.overflow_pgno
        while overflow:
            page = pager.get_page(overflow)
            overflow = int.from_bytes(page.data[:4], 'big')
            buf += page.data[4:]

        return buf[:self.ln]
