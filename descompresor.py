from argparse import ArgumentParser
from timeit import Timer
from constants import CHUNK_SIZE, REF_BYTE_LENGTH, WINDOW_SIZE
from reference import Reference

def process_chunk(chunk: bytes, window: bytearray) -> bytearray:
    """Descomprime una parte del archivo.

    Args:
        chunk (bytes): La parte del archivo que se va a descomprimir.
        window (bytes): Buffer con los últimos 1024 bytes descomprimidos anteriormente.
        Se necesitan para encontrar referencias a secuencias anteriores de bytes.

    Returns:
        bytearray: La parte descomprimida en bytes.
    """
    output = window
    
    for i in range(0, len(chunk), REF_BYTE_LENGTH):
        ref = Reference.from_bytes(chunk[i: i + REF_BYTE_LENGTH])
        match_start = len(output) - ref.offset
        output.extend(output[match_start: match_start + ref.length])
        output.append(ref.next_byte)

    return output

def decompress(filename: str, outfile: str):
    """Descomprime un archivo comprimido usando el algoritmo LZ77.

    Args:
        filename (str): Nombre del archivo comprimido.
        outfile (str): Nombre del archivo descomprimido de salida.
    """
    with open(filename, "rb") as file, open(outfile, "wb") as out:
        output = bytearray()

        while chunk := file.read(CHUNK_SIZE - 1):
            window = output[-WINDOW_SIZE:]
            window_length = len(window)
            output = process_chunk(chunk, window)
            out.write(output[window_length:])

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