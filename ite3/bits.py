
def read_varint(b):
    v = 0

    for i in range(len(b)):
        if i >= len(b):
            raise ValueError("Invarid varint")
        c = b[i]
        if i == 8:
            n = (n << 8) | c
            return n, i + 1

        n = (n << 7) | (c & 0x7f)
        if c < 0x80:
            return n, i + 1
