from __future__ import annotations

REF_BYTE_LENGTH = 3
LENGTH_BITS = 6
OFFSET_BITS = 8 * (REF_BYTE_LENGTH - 1) - LENGTH_BITS
WINDOW_SIZE = 2**OFFSET_BITS - 1
LENGTH_THRESHOLD = 2
MAX_REF_LENGTH = 2**LENGTH_BITS - 1
CHUNK_SIZE = 65536
OFFSET_MASK = WINDOW_SIZE
LENGTH_MASK = MAX_REF_LENGTH << OFFSET_BITS

class Reference:
    """Referencia a una sequencia de bytes encontrada anteriormente. Comprime la misma sequencia si se vuelve a encontrar. 

    Attributes:
        offset (int): Distancia al inicio de la anterior ocurrencia de la sequencia.
        length (int): Número de bytes en la sequencia
    """
    offset: int
    length: int
    next_byte: int

    def __init__(self, offset: int, length: int, next_byte: int) -> None:
        """Construye una referencia a una sequencia de bytes encontrada anteriormente. 

        Args:
            offset (int): Distancia al inicio de la anterior ocurrencia de la sequencia.
            length (int): Número de bytes en la sequencia.
            next_byte (int): Byte ubicado inmediatamente despues de esta secuencia.
        """
        self.offset = offset
        self.length = length
        self.next_byte = next_byte
    
    @staticmethod
    def from_bytes(buffer: bytes) -> Reference:
        """Construye una referencia a una sequencia de bytesencontrada anteriormente a partir de su representación en bytes. 

        Args:
            buffer (bytes): Representación en bytes de la referencia.

        Returns:
            Reference: Una nueva instancia de clase referencia.
        """
        next_byte = buffer[2]
        int_value = int.from_bytes(buffer[:2], "big")
        length = (int_value & LENGTH_MASK) >> OFFSET_BITS
        offset = int_value & OFFSET_MASK

        return Reference(offset, length, next_byte)

    def to_bytes(self) -> bytes:
        """Serializa la referencia y la convierte a su representanción en bytes.

        Returns:
            bytes: La referencia como una secuencia de bytes.
        """
        packed_bits = (((self.length << OFFSET_BITS) + self.offset) << 8) + self.next_byte

        return packed_bits.to_bytes(REF_BYTE_LENGTH, "big")
    
    def __str__(self) -> str:
        return f"Reference(offset={self.offset}, length={self.length}, next={self.next_byte})"