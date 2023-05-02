import os
import itertools

from timeit import Timer
from argparse import ArgumentParser
from contextlib import contextmanager
from typing import Generator
from shared import Literal, Reference, WINDOW_SIZE, CHUNK_SIZE, MAX_REF_LENGTH, MIN_BYTE_LENGTH, byte_length, progress_bar

def window_match(lookahead: str, window: str) -> Literal | Reference:
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
    progress = 0
    file_size = os.stat(filename).st_size

    with file_read(filename) as chunks, open(outfile, "wb") as out:
        if first := next(chunks, None):
            output = process_chunk(first, 0)
            out.write(output)
            progress += len(first.encode("utf-8"))
            print(progress_bar(progress, file_size), end='\r')

        for chunk in chunks:
            output = process_chunk(chunk, WINDOW_SIZE)
            out.write(output)
            progress += len(first.encode("utf-8"))
            print(progress_bar(progress, file_size), end='\r')

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