from enum import Enum
from argparse import ArgumentParser
from timeit import Timer
from shared import BITMASK, Reference

class TokenType(Enum):
    """Tipos de símbolo que pueden estar en un archivo comprimido. El valor que se le asigna a cada variante del enum es el tamaño en bytes del símbolo."""
    Reference = 0
    Char8 = 1
    Char16 = 2
    Char24 = 3
    Char32 = 4

def parse_token(byte: int) -> TokenType:
    """Determina a cual tipo de símbolo pertenece un byte.

    Args:
        byte (int): El byte a analizar.

    Raises:
        ValueError: Si el byte no es valido.

    Returns:
        TokenType: El tipo de símbolo al que pertenece el byte.
    """
    for i in range(5):
        if (BITMASK >> i) & byte == 0:
            match i:
                case 0:
                    return TokenType.Char8
                case 1:
                    return TokenType.Reference
                case 2:
                    return TokenType.Char16
                case 3:
                    return TokenType.Char24
                case _:
                    return TokenType.Char32
    
    raise ValueError(f"Byte {byte} is not a valid compressed token.")

def decompress(filename: str, outfile: str):
    """Descomprime un archivo comprimido con una versión modificada del algoritmo LZ77.

    Args:
        filename (str): Nombre del archivo comprimido.
        outfile (str): Nombre del archivo descomprimido de salida.
    """
    with open(filename, "rb") as file:
        buffer = file.read()
        output: list[str] = []
        i = 0

        while i < len(buffer):
            token_type = parse_token(buffer[i])

            match token_type:
                case TokenType.Reference:
                    ref = Reference(buffer=bytes(buffer[i: i + 2]))
                    match_start = len(output) - ref.offset
                    output.extend(output[match_start: match_start + ref.length])
                    i += 2
                case TokenType.Char8 | TokenType.Char16 | TokenType.Char24 | TokenType.Char32:
                    char = str(bytes(buffer[i: i + token_type.value]), encoding="utf-8")
                    output.append(char)
                    i += token_type.value
            
    with open(outfile, "w") as out:
        out.write("".join(output))

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Decompresor",
        description="Descomprime un archivo comprimido"
    )

    parser.add_argument("-z", "--zipfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo descomprimido", default="descomprimido-elmejorprofesor.txt")

    args = parser.parse_args()
    filename, outfile = args.zipfile, args.outfile
    timer = Timer(lambda: decompress(filename, outfile))

    print(f"Tiempo de ejecución: {timer.timeit(1)} segundos")