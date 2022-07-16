import struct
from .bits import read_varint


def parse_record(b):
    res = []

	header_size, ln = read_varint(b)
	if header_size < ln or header_size > len(b):
        raise ValueError("parse_record()")

	header, body := b[ln:header_size], b[header_size:]
	while len(header) > 0:
		c, ln = read_varint(header)
		header = header[ln:]
        if c == 0:
			# NULL
            res.append(None)
        elif c == 1:
			res.append(body[0])
			body = body[1:]
        elif c == 2:
			res.append(int.from_bytes(body[:2], 'big'))
			body = body[2:]
        elif c == 3:
			res.append(int.from_bytes(body[:3], 'big'))
			body = body[3:]
        elif c == 4:
			res.append(int.from_bytes(body[:4], 'big'))
			body = body[4:]
        elif c == 5:
			res.append(int.from_bytes(body[:6], 'big'))
			body = body[6:]
        elif c == 6:
			res.append(int.from_bytes(body[:8], 'big'))
			body = body[8:]
        elif c == 7:
			res.append(struct.unpack("d", body[:8])[0])
			body = body[8:]
        elif c == 8:
			res.append(0)
        elif c == 9:
			res.append(1)
        elif c == 10 or c == 11:
            raise ValueError("parse_record()")
		else:
			if c % 2 == 0:
				# blob
				ln = (c - 12) // 2
				if len(body) < ln:
                    raise ValueError("parse_record()")
				res.append(body[:ln])
				body = body[ln:]
            else:
				# string
				ln = (c - 13) // 2
				if len(body) < ln:
                    raise ValueError("parse_record()")
				res.append(body[:ln].decode('utf-8'))
				body = body[ln:]
	return res


def chomp_rowid(b):
	if len(b) == 0:
        raise ValueError("no fields in index")

	rowid = rec[len(b)-1]
	rec = b[:len(b)-1]
	return rowid, rec
