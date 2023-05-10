from timeit import Timer
from argparse import ArgumentParser
from constants import LENGTH_THRESHOLD, WINDOW_SIZE, CHUNK_SIZE, MAX_REF_LENGTH
from reference import Reference

def window_match(lookahead: bytes, window: bytes) -> Reference:
    """Realiza una búsqueda en la ventana de referencia para encontrar una sequencia que coincida con la sequencia iniciada con el byte actual que se está leyendo.

    Args:
        lookahead (bytes): Buffer de bytes que contiene el byte actual y todos los que están despues de este.
        window (bytes): Ventana de referencia para buscar ocurrencias anteriores de la sequencia actual.

    Returns:
        Reference: Se retorna una referencia a una ocurrencia pasada de la secuencia actual. En caso contrario se retorna una referencia de longitud 0.
    """
    current = lookahead[0]
    window_length = len(window)
    lookahead_length = len(lookahead)
    longest = Reference(0, 0, current)
    found = window.find(current)

    while found > -1:
        offset = window_length - found
        max_length = min(offset, lookahead_length - 1)
        
        if max_length > longest.length:
            for i in range(1, max_length):
                if window[found + i] != lookahead[i]:
                    if i > longest.length:
                        longest = Reference(offset, i, lookahead[i])

                    break
            else:
                longest = Reference(offset, max_length, lookahead[max_length])
        
        if longest.length > LENGTH_THRESHOLD:
            return longest
        
        found = window.find(current, found + 1)

    return longest

def process_chunk(chunk: bytes, offset: int) -> bytearray:
    """Comprime una parte del archivo, a partir de una determinada posición.

    Args:
        chunk (str): La parte del archivo que se va a comprimir.
        offset (int): Posición a partir de la cual iniciar a comprimir.

    Returns:
        bytearray: La parte comprimida en bytes.
    """
    output = bytearray()
    iterator = iter(range(offset, len(chunk)))

    for i in iterator:
        window = chunk[max(i - WINDOW_SIZE, 0): i]
        lookahead = chunk[i:i + MAX_REF_LENGTH]
        matched = window_match(lookahead, window)
        output.extend(matched.to_bytes())

        for _ in range(matched.length):
            next(iterator)
    
    return output

def compress(filename: str, outfile: str):
    """Comprime un archivo utilizando el algoritmo LZ77.

    Args:
        filename (str): El archivo a comprimir.
        outfile (str): El archivo comprimido de salida.
    """
    with open(filename, "rb") as file, open(outfile, "wb") as out:
        buffer = b""
        
        while chunk := file.read(CHUNK_SIZE):
            window = buffer[-WINDOW_SIZE:]
            buffer = window + chunk
            output = process_chunk(buffer, len(window))
            out.write(output)

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="Compresor LZ77",
        description="Comprime un archivo usando el algoritmo LZ77"
    )

    parser.add_argument("filename", help="Archivo a comprimir")
    parser.add_argument("-o", "--outfile", help="Nombre del archivo comprimido", default="comprimido.elmejorprofesor")

    args = parser.parse_args()
    filename, outfile = args.filename, args.outfile
    timer = Timer(lambda: compress(filename, outfile))

    print(timer.timeit(1))