LENGTH_BITS = 5
OFFSET_BITS = 14 - LENGTH_BITS
WINDOW_SIZE = 2**OFFSET_BITS - 1
MIN_BYTE_LENGTH = 2
MAX_REF_LENGTH = 2**LENGTH_BITS - 1
CHUNK_SIZE = 65536
BITMASK = 0x80
OFFSET_MASK = WINDOW_SIZE << LENGTH_BITS
LENGTH_MASK = MAX_REF_LENGTH

class Literal:
    value: str

    def __init__(self, value: str) -> None:
        self.value = value

    def to_bytes(self) -> bytes:
        return bytes(self.value, "utf-8")
    
    def step(self) -> int:
        return 1
    
    def __str__(self) -> str:
        return f"Literal({self.value})"

class Reference:
    offset: int
    length: int

    def __init__(self, offset: int = 0, length: int = 0, buffer: bytes | None = None) -> None:
        if buffer:
            int_value = int.from_bytes(buffer)
            self.offset = (int_value & OFFSET_MASK) >> LENGTH_BITS
            self.length = int_value & LENGTH_MASK
        else:
            self.offset = offset
            self.length = length
    
    def to_bytes(self) -> bytes:
        packed_bits = 0x8000 + (self.offset << LENGTH_BITS) + self.length

        return packed_bits.to_bytes(2)
    
    def step(self) -> int:
        return self.length
    
    def __str__(self) -> str:
        return f"Reference(offset={self.offset}, length={self.length})"

def byte_length(string: str) -> int:
    return len(bytes(string, "utf-8"))

def progress_bar(progress: int, total: int) -> str:
    percent = min(progress * 100 / total, 100)

    return f"[{'#' * int(percent)}{' ' * int(100 - percent)}] {round(percent, 1)}%"