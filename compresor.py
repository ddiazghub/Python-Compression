import os

from timeit import Timer
from argparse import ArgumentParser
from contextlib import contextmanager
from typing import Generator
from shared import Literal, Reference, WINDOW_SIZE, CHUNK_SIZE, MAX_REF_LENGTH, MIN_BYTE_LENGTH, byte_length

def window_match(lookahead: str, window: str) -> Literal | Reference:
    """Realiza una búsqueda en la ventana de referencia para encontrar una sequencia que coincida con la sequencia iniciada con el carácter actual que se está leyendo.
    Si existe una sequencia anterior lo suficientemente larga para ser comprimida y ahorrar espacio, se comprime.

    Args:
        lookahead (str): Cadena que contiene el carácter actual y todos los que están despues de este.
        window (str): Ventana de referencia para buscar ocurrencias anteriores de la sequencia actual.

    Returns:
        Literal | Reference: Si se encuentra una ocurrencia anterior de la sequencia actual y se puede ahorrar espacio representándola como una referencia, se retorna un objeto de clase Reference.
        En caso contrario, se retorna un literal y el caractér no se comprime.
    """
    window_length = len(window)
    lookahead_length = len(lookahead)
    found = window.find(lookahead[0])

    while found > -1:
        max_length = min(window_length - found, lookahead_length, MAX_REF_LENGTH)

        for i in range(1, max_length):
            if window[found + i] != lookahead[i]:
                if byte_length(lookahead[:i]) > MIN_BYTE_LENGTH:
                    return Reference(offset=window_length - found, length=i)
                
                break
        else:
            if byte_length(lookahead[:max_length]) > MIN_BYTE_LENGTH:
                return Reference(offset=window_length - found, length=max_length)
        
        found = window.find(lookahead[0], found + 1)

    return Literal(lookahead[0])

@contextmanager
def file_read(filename: str) -> Generator[Generator[str, None, None], None, None]:
    """Función que ayuda a leer un archivo por partes con un buffer.

    Args:
        filename (str): El archivo a leer.
    """
    file = open(filename, "r")

    try:
        def reader() -> Generator[str, None, None]:
            read = file.read(CHUNK_SIZE)
            text = ""

            while len(read) > 0:
                text = text[-WINDOW_SIZE:] + read
                yield text
                read = file.read(CHUNK_SIZE)

        yield reader()
    finally:
        file.close()

def process_chunk(chunk: str, offset: int) -> bytearray:
    """Comprime una parte del archivo de texto, a partir de una determinada posición.

    Args:
        chunk (str): La parte del archivo de texto que se va a comprimir.
        offset (int): Posición a partir de la cual iniciar a comprimir.

    Returns:
        bytearray: La parte comprimida en bytes.
    """
    output = bytearray()
    iterator = iter(range(offset, len(chunk)))

    for i in iterator:
        window = chunk[max(i - WINDOW_SIZE, 0): i]
        lookahead = chunk[i:]
        matched = window_match(lookahead, window)
        output.extend(matched.to_bytes())

        if isinstance(matched, Reference):
            for _ in range(matched.length - 1):
                next(iterator)

    return output

def compress(filename: str, outfile: str):
    """Comprime un archivo de texto utilizando una versión modificada del algoritmo LZ77. Muestra una barra de progreso mientras se está haciendo la compresión.

    Args:
        filename (str): El archivo de texto.
        outfile (str): El archivo comprimido de salida.
    """
    progress = 0
    file_size = os.stat(filename).st_size

    with file_read(filename) as chunks, open(outfile, "wb") as out:
        # Primera parte del archivo.
        if first := next(chunks, None):
            output = process_chunk(first, 0)
            out.write(output)
            progress += len(first.encode("utf-8"))
            #print(progress_bar(progress, file_size), end='\r')

        # Siguientes partes del archivo.
        for chunk in chunks:
            output = process_chunk(chunk, WINDOW_SIZE)
            out.write(output)
            progress += len(first.encode("utf-8"))
            #print(progress_bar(progress, file_size), end='\r')

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor",
        description="Comprime un archivo de texto"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")

    args = parser.parse_args()
    filename, outfile = args.filename, args.outfile
    timer = Timer(lambda: compress(filename, outfile))

    print(f"\nTiempo de ejecución: {timer.timeit(1)} segundos")