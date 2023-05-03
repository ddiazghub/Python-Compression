LENGTH_BITS = 6
OFFSET_BITS = 14 - LENGTH_BITS
WINDOW_SIZE = 2**OFFSET_BITS - 1
MIN_BYTE_LENGTH = 2
MAX_REF_LENGTH = 2**LENGTH_BITS - 1
CHUNK_SIZE = 65536
BITMASK = 0x80
OFFSET_MASK = WINDOW_SIZE << LENGTH_BITS
LENGTH_MASK = MAX_REF_LENGTH

class CompressionToken:
    """Símbolo que representa a uno o más caractéres de texto en un archivo comprimido"""

    def to_bytes(self) -> bytes:
        """Convierte el símbolo a su representación en bytes"""
        pass
    
    def step(self) -> int:
        """Número de caracteres a saltar antes de leer el siguiente carácter."""
        pass

class Literal(CompressionToken):
    """Caractér literal que no será comprimido

    Attributes:
        value (str): El valor del carácter
    """
    value: str

    def __init__(self, value: str) -> None:
        """Carácter literal que no será comprimido

        Attributos:
            value (str): El valor del carácter
        """
        self.value = value

    def to_bytes(self) -> bytes:
        return bytes(self.value, "utf-8")
    
    def step(self) -> int:
        return 1
    
    def __str__(self) -> str:
        return f"Literal({self.value})"

class Reference(CompressionToken):
    """Referencia a una sequencia de caracteres encontrada anteriormente. Comprime la misma sequencia si se vuelve a encontrar. 

    Attributes:
        offset (int): Distancia al inicio de la anterior ocurrencia de la sequencia.
        length (int): Número de caracteres en la sequencia
    """
    offset: int
    length: int

    def __init__(self, offset: int = 0, length: int = 0, buffer: bytes | None = None) -> None:
        """Construye una referencia a una sequencia de caracteres encontrada anteriormente. 

        Args:
            offset (int): Distancia al inicio de la anterior ocurrencia de la sequencia.
            length (int): Número de caracteres en la sequencia.
            buffer (bytes | None, optional): Representación en bytes de la referencia.
            Si se da este parámetro, se construye una referencia a partir de los bytes.
        """
        if buffer:
            int_value = int.from_bytes(buffer, "big")
            self.offset = (int_value & OFFSET_MASK) >> LENGTH_BITS
            self.length = int_value & LENGTH_MASK
        else:
            self.offset = offset
            self.length = length
    
    def to_bytes(self) -> bytes:
        packed_bits = 0x8000 + (self.offset << LENGTH_BITS) + self.length

        return packed_bits.to_bytes(2, "big")
    
    def step(self) -> int:
        return self.length
    
    def __str__(self) -> str:
        return f"Reference(offset={self.offset}, length={self.length})"

def byte_length(string: str) -> int:
    """Longitud en bytes de una cadena.

    Args:
        string (str): La cadena de texto.

    Returns:
        int: Su longitud en bytes.
    """
    return len(bytes(string, "utf-8"))


def progress_bar(progress: int, total: int) -> str:
    """Retorna una barra de progreso como una cadena de texto.

    Args:
        progress (int): Progreso actual.
        total (int): El total o valor de progreso necesario para terminar.

    Returns:
        str: Una barra de progreso como una cadena de texto.
    """
    percent = min(progress * 100 / total, 100)

    return f"[{'#' * int(percent)}{' ' * int(100 - percent)}] {round(percent, 1)}%"